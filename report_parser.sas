/* Собираем данные обо всех пользователях */
data users_grps;
	length uri name dispname group groupuri $256 
		id MDUpdate $20;
	n=1;
	call missing(uri, name, dispname, group, groupuri, id, MDUpdate);
	nobj=metadata_getnobj("omsobj:Person?@Id contains '.'",n,uri);

	if nobj=0 then
		put 'No Persons available.';
	else
		do while (nobj > 0);
			rc=metadata_getattr(uri, "Name", Name);
			rc=metadata_getattr(uri, "DisplayName", DispName);
			a=1;
			grpassn=metadata_getnasn(uri,"IdentityGroups",a,groupuri);

			if grpassn in (-3,-4) then
				do;
					group="No groups";
					output;
				end;
			else
				do while (grpassn > 0);
					rc2=metadata_getattr(groupuri, "Name", group);
					rc=metadata_getattr(groupuri, "MetadataUpdated", MDUpdate);
					a+1;
					output;
					grpassn=metadata_getnasn(uri,"IdentityGroups",a,groupuri);
				end;

			n+1;
			nobj=metadata_getnobj("omsobj:Person?@Id contains '.'",n,uri);
		end;

	keep name dispname MDUpdate group;
run;

/* Указываем путь до выгруженного файла из парсера и импортируем его*/
filename imp_file '/tmp/result.csv';

proc import datafile=imp_file
	out=WORK.imp_file
	dbms=csv
	replace;
	delimiter=';';
	getnames=yes;
run;

/* Объединяем данные всех пользователей и из импортированного файла */
proc sql;
	create table work.imp_file as 
		select 
			t1.name, 
			t1.dispname, 
			t1.group, 
			t2.last_connect_dttm
		from work.users_grps t1 
			left join work.imp_file t2 on t1.name = t2.Users
/*		order by t2.last_connect_dttm */
	;
quit;

/* Схлопываем строки с группами в одну */
data work.result(rename=(new_row=group));
	do until(last.name);
		set work.imp_file;
		by name;
		length new_row $5000.;
		new_row=catx(', ',new_row, group);
	end;

	drop group;
run;

/* Сортируем полученные данные, по дате (по убыванию) */
proc sort data=work.result out=work.result_sorted;
    by descending last_connect_dttm ;
run;