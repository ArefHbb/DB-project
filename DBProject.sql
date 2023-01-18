-- drop view
drop view check_valid_natcod;

-- drop tables
drop table trn_src_des;
drop table branch;
drop table deposit;
drop table deposit_status;
drop table deposit_type;
drop table customer;
drop table final_table;

-- create tables
create table customer(
	cid int primary key,
	name varchar(20),
	natcod varchar(10),
	birthdate Date,
	add varchar(50),
	tel varchar(20)
);

create table deposit_type(
	dep_type int primary key,
	dep_type_desc varchar(50)
);

create table deposit_status(
	status int primary key,
	status_desc varchar(50)
);

create table deposit(
	dep_id int primary key,
	dep_type int,
	cid int,
	opendate date,
	status int,
	foreign key (dep_type) references deposit_type(dep_type),
	foreign key (status) references deposit_status(status),
	foreign key (cid) references customer(cid)
);

create table branch(
	branch_id int primary key,
	branch_name varchar(20),
	branch_add varchar(20),
	branch_tel varchar(20)
);

create table trn_src_des(
	voucherid varchar(10) primary key,
	trndate date,
	trntime varchar(10),
	amount bigint,
	sourcedep int,
	desdep int,
	branch_id int,
	trn_desc varchar(50),
	foreign key (branch_id) references branch(branch_id)
-- 	foreign key (sourcedep) references deposit(dep_id),
-- 	foreign key (desdep) references deposit(dep_id)
);

create table final_table(voucherid varchar(10));

insert into customer values(1,'aref habibi','1130579425','1380/12/18','','');
insert into customer values(2,'mohammad mohammadi','0441016359','1380/12/14','','');
insert into customer values(3,'ali karimi','1111122222','1370/2/9','','');

-- 1)
create or replace view check_valid_natcod AS with temp as (select *, ((cast(SUBSTRING(natcod, 1, 1) as int) * 10) +
					  (cast(SUBSTRING(natcod, 2, 1) as int) * 9) +
					  (cast(SUBSTRING(natcod, 3, 1) as int) * 8) +
					  (cast(SUBSTRING(natcod, 4, 1) as int) * 7) +
					  (cast(SUBSTRING(natcod, 5, 1) as int) * 6) +
					  (cast(SUBSTRING(natcod, 6, 1) as int) * 5) +
					  (cast(SUBSTRING(natcod, 7, 1) as int) * 4) +
					  (cast(SUBSTRING(natcod, 8, 1) as int) * 3) +
					  (cast(SUBSTRING(natcod, 9, 1) as int) * 2)) % 11 correct_num -- correct_num adadi ke az hesab kardan baghie adad ha be dast miad 
from customer)
select cid, name, natcod, birthdate, add, tel, case
												when correct_num < 2 and correct_num = cast(SUBSTRING(natcod, 10, 1) as int) then true
												when correct_num >= 2 and 11 - correct_num = cast(SUBSTRING(natcod, 10, 1) as int) then true
												else false
												end isvalid_natcod -- hesab kardan dorost ragham akhar tebgh correct_num v moghayese ba ragham akhar code meli
from temp;

select * from check_valid_natcod;


-- 2)
insert into deposit_type values(1,'');
insert into deposit_status values(1,'');

insert into branch values(1,'','','');

insert into deposit values(1,1,1,'1390-1-1',1);
insert into deposit values(2,1,1,'1390-1-1',1);
insert into deposit values(3,1,1,'1390-1-1',1);
insert into deposit values(4,1,1,'1390-1-1',1);
insert into deposit values(5,1,1,'1390-1-1',1);
insert into deposit values(6,1,1,'1390-1-1',1);
insert into deposit values(7,1,1,'1390-1-1',1);
insert into deposit values(8,1,1,'1390-1-1',1);
insert into deposit values(9,1,1,'1390-1-1',1);

insert into trn_src_des values('1','1398-11-1','100101',19,101,1,1,'');
insert into trn_src_des values('2','1398-12-20','100101',65,102,2,1,'');
insert into trn_src_des values('3','1398-12-1','100101',20,1,3,1,'');
insert into trn_src_des values('4','1398-12-11','100101',20,null,3,1,'');
insert into trn_src_des values('5','1398-12-21','100101',60,2,3,1,'');
insert into trn_src_des values('6','1399-1-1','110101',100,3,4,1,'');
insert into trn_src_des values('7','1399-1-2','110101',100,4,5,1,'');
insert into trn_src_des values('8','1399-1-2','110102',40,4,6,1,'');
insert into trn_src_des values('9','1399-1-2','110103',50,4,7,1,'');
insert into trn_src_des values('10','1399-1-3','110101',20,4,8,1,'');
insert into trn_src_des values('11','1399-1-3','110101',30,8,9,1,'');
insert into trn_src_des values('12','1399-1-3','110102',25,8,108,1,'');
insert into trn_src_des values('13','1399-1-4','110101',30,9,null,1,'');

insert into trn_src_des values('14','1398-10-1','110101',15,200,101,1,'');
insert into trn_src_des values('15','1399-1-3','110105',10,300,5,1,'');
insert into trn_src_des values('16','1399-1-5','110105',10,108,400,1,'');


create or replace procedure next_transaction (_voucherid varchar(10)) -- mohasebe tarakonesh haye badi
	language plpgsql as
	$$
		declare 
			sum_amount bigint; -- majmooe tarakonesh haye dar tr table
			temp_trndate date; -- A day
			temp_amount bigint; -- meghdar tarakonesh
			temp_desdep int; -- destination tarakonesh
			rec record; -- record baraye loop
		begin
		
			select desdep, amount, trndate into temp_desdep, temp_amount, temp_trndate -- maghadir (temp_desdep, temp_amount, temp_trndate) ra be dast miavarad
			from trn_src_des
			where voucherid = _voucherid; 
			
			if temp_desdep is not null and temp_desdep in (select dep_id from deposit) then
				select trndate into temp_trndate -- meghdar vagheie rooz A da soal ra da temp_trndate mirizim
				from (select *, row_number() over(order by trndate) rownumber
						from trn_src_des
						where temp_trndate <= trndate and voucherid != _voucherid and temp_desdep = trn_src_des.sourcedep) t
				where rownumber = 1;
				
				-- temp_table jadvali ast k hame tarakonesh haee k tebgh mohasebat dar in function be dast ovordim ra dar in jadval vared mikonim
				insert into temp_table select voucherid from trn_src_des where temp_trndate = trndate and amount = temp_amount and temp_desdep = trn_src_des.sourcedep;

				-- tr_table jadval tr k dar soal zekr shode
				insert into tr_table select voucherid, amount from trn_src_des where temp_trndate = trndate and amount != temp_amount and temp_desdep = trn_src_des.sourcedep;

				select sum(amount) into sum_amount from tr_table; -- majmooe maghadir tr ra dar amount mirizim

				-- tamam bardasht haye anjam shode az dest ke bad az rooz A b dast oomade ra bar hasb zaman ta jaee k sum<amount edame midahim
				for rec in select voucherid, amount
							from trn_src_des 
							where temp_trndate < trndate and temp_desdep = trn_src_des.sourcedep
							order by trndate, trntime
					loop
						if sum_amount >= temp_amount then exit; end if;

						if rec.amount + sum_amount <= temp_amount * 1.1
							then insert into tr_table values(rec.voucherid, rec.amount);
								 sum_amount = sum_amount + rec.amount;
						end if;
					end loop;

				insert into temp_table select voucherid from tr_table;
			end if;
			delete from tr_table;
		end;
	$$;

-- tamam comment ha dar in tabe mesl tabe bala hast
create or replace procedure prev_transaction (_voucherid varchar(10))
	language plpgsql as $$
		declare 
			sum_amount bigint;
			temp_trndate date;
			temp_amount bigint;
			temp_sourcedep int;
			rec record;
		begin
		
			select sourcedep, amount, trndate into temp_sourcedep, temp_amount, temp_trndate
			from trn_src_des
			where voucherid = _voucherid;
		
			if temp_sourcedep is not null and temp_sourcedep in (select dep_id from deposit) then
				select trndate into temp_trndate
				from (select *, row_number() over(order by trndate desc) rownumber
						from trn_src_des
						where temp_trndate >= trndate and voucherid != _voucherid and temp_sourcedep = trn_src_des.desdep) t
				where rownumber = 1;

				insert into temp_table select voucherid from trn_src_des where temp_trndate = trndate and amount = temp_amount and temp_sourcedep = trn_src_des.desdep;

				insert into tr_table select voucherid, amount from trn_src_des where temp_trndate = trndate and amount != temp_amount and temp_sourcedep = trn_src_des.desdep;

				select sum(amount) into sum_amount from tr_table;


				for rec in select voucherid, amount 
							from trn_src_des 
							where temp_trndate > trndate and temp_sourcedep = trn_src_des.desdep
							order by trndate, trntime
					loop
						if sum_amount >= temp_amount then exit; end if;

						if rec.amount + sum_amount <= temp_amount * 1.1
							then insert into tr_table values(rec.voucherid, rec.amount);
									sum_amount = sum_amount + rec.amount;
						end if;
					end loop;

				insert into temp_table select voucherid from tr_table;
			end if;
			delete from tr_table;
		end;
	$$;


create or replace procedure find_transaction (_voucherid varchar(10))
	language plpgsql as $$
	declare
		i record;
	begin
		-- sakhtan table haye mored niaz dar tabe haye bala
		create table temp_table(voucherid varchar(10));
		create table tr_table(voucherid varchar(10), amount bigint);
		create table A_table(voucherid varchar(10));
		create table B_table(voucherid varchar(10));
		
		insert into final_table values(_voucherid);
		
		call next_transaction(_voucherid); -- next transaction ra baraye tarakonesh asli hesab mikonim
		insert into a_table select * from temp_table;
		delete from temp_table;
		
		-- next transaction ra baraye tarakonesh haye badi enghadr call mikonim ta dar hame shakhe ha be enteha beresim
		while 1 loop
			for i in select * from a_table
				loop
					call next_transaction(i.voucherid);
					insert into b_table select * from temp_table;
					delete from temp_table;
				end loop;
			insert into final_table select * from a_table;
			delete from a_table;
			
			if (select count(*) from b_table) = 0 then exit; end if;
			
			insert into a_table select * from b_table;
			delete from b_table;
		end loop;
		
		call prev_transaction(_voucherid); -- previous transaction ra baraye tarakonesh asli hesab mikonim
		insert into a_table select * from temp_table;
		delete from temp_table;
		
		-- previous transaction ra baraye tarakonesh haye badi enghadr call mikonim ta dar hame shakhe ha be enteha beresim
		while 1 loop
			for i in select * from a_table
				loop
					call prev_transaction(i.voucherid);
					insert into b_table select * from temp_table;
					delete from temp_table;
				end loop;
			insert into final_table select * from a_table;
			delete from a_table;
			
			if (select count(*) from b_table) = 0 then exit; end if;
			
			insert into a_table select * from b_table;
			delete from b_table;
		end loop;
		
		drop table temp_table;
		drop table tr_table;
		drop table a_table;
		drop table b_table;
	end;$$;
	
call find_transaction('6');
select * from final_table;






