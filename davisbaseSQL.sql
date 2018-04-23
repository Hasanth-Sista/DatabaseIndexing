create table project ( Pname TEXT PRIMARY KEY,  Pnumber INT,  Plocation TEXT );

insert into project (Pname,Pnumber,Plocation) values ("DatabaseDesign",1,"Dallas");
insert into project (Pname,Pnumber,Plocation) values ("WebProject",2,"Austin");
insert into project (Pname,Pnumber,Plocation) values ("SpaceX",3,"Houston");

update project set pname = "TechStoreProject" where pnumber = 1;

delete from project where pnumber > 0;

select * from project;
select * from project where pnumber < 3;

create table department ( Dno INT PRIMARY KEY,  Dname TEXT );

insert into department (Dno,Dname) values (1,"ComputerScience");
insert into department (Dno,Dname) values (2,null);
insert into department (Dno,Dname) values (3,"JindalSchoolOfManagement");

update department set dname = "Electrical" where dno = 3;

delete from department where dno = 2;

select * from department;
select * from department where dno > 1;
