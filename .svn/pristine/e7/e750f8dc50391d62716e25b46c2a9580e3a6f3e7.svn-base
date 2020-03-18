WHENEVER SQLERROR EXIT SQL.SQLCODE
set pages 0
set head off
set feed off
set verify off
set serveroutput off
set echo off
 
define cdr_schm=&1
define grpid=&2
define base_tbl_nm=&3
define client_ds_id=&4
 
variable v_temp_table varchar2(30);
 
begin
  &cdr_schm..LOAD_EXTERNAL_DATA_PKG.create_temp_load_table (p_groupid => '&grpid'
                                                        ,p_base_table_name => '&base_tbl_nm'
                                                        ,p_client_ds_id => &client_ds_id
							,p_trunc_if_exists => 'Y'	
                                                        ,p_temp_load_table => :v_temp_table
                                                        );
end;
/
 
PRINT :v_temp_table
exit
