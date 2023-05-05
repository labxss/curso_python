
# destino


echo "
drop materialized view if exists bd_sabeis.vw_siasus_pa_diretriz_usuario_sintese ;
create materialized view bd_sabeis.vw_siasus_pa_diretriz_usuario_sintese as
select nu_cnspcn usuariosus,
       count(*) qt_registro_total,
       sum(case when sg_sexo = 'F' then 1 else 0 end) sexo_f_total,
       min(nu_idade) idade_min_total,
       max(nu_idade) idade_max_total,
       min(co_seq_competencia) co_mes_min_total,
       max(co_seq_competencia) co_mes_max_total,
       string_agg(distinct co_seq_diretriz::text,', ') as co_seq_diretriz_total,
       string_agg(distinct co_seq_cidpri::text,', ') co_seq_cidpri_total,
       string_agg(distinct co_seq_procedimento::text,', ') co_seq_procedimento_total,
       string_agg(distinct co_seq_gestao::text,', ') co_seq_gestao_total,
       string_agg(distinct co_cnes_estabelecimento::text,', ') co_cnes_estabelecimento_total,
       string_agg(distinct co_seq_municipio_residencia::text,', ') co_seq_municipio_residencia_total,
       sum(vl_aprovado) vl_aprovado_total,
       string_agg(distinct replace(ds_esquema_1,',',';'),', ') qt_esquema1_total,
       string_agg(distinct replace(ds_esquema_2,',',';'),', ') qt_esquema2_total          
  from bd_sabeis.tf_siasus_pa_diretriz
 where nu_cnspcn > 0
 group by 1;
" # | PGPASSWORD=$pw psql -U $us -h $hs -d $dbase -p $pt -e


for tb in $(echo "select 'SELECT ''' || table_schema || '.' || table_name  || ''' origem, COUNT(*) qt_registro FROM (select * from ' || table_schema || '.' || table_name || ' limit 100)x;' from information_schema.tables where table_schema = 'bd_sabeis_diretriz' order by 1" | PGPASSWORD=$pw psql -U $us -h $hs -d $dbase -p $pt -t --csv |sed 's/"//g' | PGPASSWORD=$pw psql -U $us -h $hs -d $dbase -p $pt -t --csv | grep "100$" | awk -F',' '{print $1}' | sort -h);do 

   tb1=$(echo $tb | sed 's/tf_/vw_/g')

echo "
drop materialized view if exists ${tb1}_usuario_sintese ;
create materialized view ${tb1}_usuario_sintese as
with subset as (
select nu_cnspcn usuariosus,
       count(*) qt_registro,
       sum(case when sg_sexo = 'F' then 1 else 0 end) sexo_f,
       min(nu_idade) idade_min,
       max(nu_idade) idade_max,
       min(co_seq_competencia) co_mes_min,
       max(co_seq_competencia) co_mes_max,
       string_agg(distinct co_seq_diretriz::text,', ') as co_seq_diretriz,
       string_agg(distinct co_seq_cidpri::text,', ') co_seq_cidpri,       
       string_agg(distinct co_seq_procedimento::text,', ') co_seq_procedimento,
       string_agg(distinct co_seq_gestao::text,', ') co_seq_gestao,
       string_agg(distinct co_cnes_estabelecimento::text,', ') co_cnes_estabelecimento,
       string_agg(distinct co_seq_municipio_residencia::text,', ') co_seq_municipio_residencia,
       sum(vl_aprovado) vl_aprovado,
       string_agg(distinct replace(ds_esquema_1,',',';'),', ') qt_esquema1,
       string_agg(distinct replace(ds_esquema_2,',',';'),', ') qt_esquema2,    
       case
         when nu_cnspcn in (
                           select distinct nu_cnspcn
                             from (
                             with subset as (
                           select distinct
                                  nu_cnspcn,
                                  co_seq_competencia,
                                  DENSE_RANK () OVER ( 
                                    PARTITION BY nu_cnspcn
                                    ORDER BY co_seq_competencia 
                                  ) co_atendimento 
                             from $tb A
                             where qt_aprovada > 0
                            order by 1,2
                            ) 
                            select A.*, B.co_seq_competencia - A.co_seq_competencia intervalo
                              from subset A, subset B
                             where A.nu_cnspcn = B.nu_cnspcn
                               and A.co_atendimento = B.co_atendimento-1
                             ) x
                             where intervalo > 6
                           ) then 1 else 0 end intervalo6meses,
       case
         when nu_cnspcn in (
                           select distinct nu_cnspcn
                             from (
                             with subset as (
                           select distinct
                                  nu_cnspcn,
                                  co_seq_competencia,
                                  DENSE_RANK () OVER ( 
                                    PARTITION BY nu_cnspcn
                                    ORDER BY co_seq_competencia 
                                  ) co_atendimento 
                             from $tb A
                             where qt_aprovada > 0
                            order by 1,2
                            ) 
                            select A.*, B.co_seq_competencia - A.co_seq_competencia intervalo
                              from subset A, subset B
                             where A.nu_cnspcn = B.nu_cnspcn
                               and A.co_atendimento = B.co_atendimento-1
                             ) x
                             where intervalo > 12
                           ) then 1 else 0 end intervalo12meses,
       case
         when nu_cnspcn in (
                           select distinct nu_cnspcn
                             from (
                             with subset as (
                           select distinct
                                  nu_cnspcn,
                                  co_seq_competencia,
                                  DENSE_RANK () OVER ( 
                                    PARTITION BY nu_cnspcn
                                    ORDER BY co_seq_competencia 
                                  ) co_atendimento 
                             from $tb A
                             where qt_aprovada > 0
                            order by 1,2
                            ) 
                            select A.*, B.co_seq_competencia - A.co_seq_competencia intervalo
                              from subset A, subset B
                             where A.nu_cnspcn = B.nu_cnspcn
                               and A.co_atendimento = B.co_atendimento-1
                             ) x
                             where intervalo > 24
                           ) then 1 else 0 end intervalo24meses,
       case
         when nu_cnspcn in (
                           select distinct nu_cnspcn
                             from (
                             with subset as (
                           select distinct
                                  nu_cnspcn,
                                  co_seq_competencia,
                                  DENSE_RANK () OVER ( 
                                    PARTITION BY nu_cnspcn
                                    ORDER BY co_seq_competencia 
                                  ) co_atendimento 
                             from $tb A
                             where qt_aprovada > 0
                            order by 1,2
                            ) 
                            select A.*, B.co_seq_competencia - A.co_seq_competencia intervalo
                              from subset A, subset B
                             where A.nu_cnspcn = B.nu_cnspcn
                               and A.co_atendimento = B.co_atendimento-1
                             ) x
                             where intervalo > 36
                           ) then 1 else 0 end intervalo36meses,
       case
         when nu_cnspcn in (
                           select distinct nu_cnspcn
                             from (
                             with subset as (
                           select distinct
                                  nu_cnspcn,
                                  co_seq_competencia,
                                  DENSE_RANK () OVER ( 
                                    PARTITION BY nu_cnspcn
                                    ORDER BY co_seq_competencia 
                                  ) co_atendimento 
                             from $tb A
                             where qt_aprovada > 0
                            order by 1,2
                            ) 
                            select A.*, B.co_seq_competencia - A.co_seq_competencia intervalo
                              from subset A, subset B
                             where A.nu_cnspcn = B.nu_cnspcn
                               and A.co_atendimento = B.co_atendimento-1
                             ) x
                             where intervalo > 48
                           ) then 1 else 0 end intervalo48meses,
       case
         when nu_cnspcn in (
                           select distinct nu_cnspcn
                             from (
                             with subset as (
                           select distinct
                                  nu_cnspcn,
                                  co_seq_competencia,
                                  DENSE_RANK () OVER ( 
                                    PARTITION BY nu_cnspcn
                                    ORDER BY co_seq_competencia 
                                  ) co_atendimento 
                             from $tb A
                             where qt_aprovada > 0
                            order by 1,2
                            ) 
                            select A.*, B.co_seq_competencia - A.co_seq_competencia intervalo
                              from subset A, subset B
                             where A.nu_cnspcn = B.nu_cnspcn
                               and A.co_atendimento = B.co_atendimento-1
                             ) x
                             where intervalo > 60
                           ) then 1 else 0 end intervalo60meses                           
  from $tb
   where nu_cnspcn > 0
 group by 1)
 select A.*, 
        B.qt_registro_total,
        B.sexo_f_total,
        B.idade_min_total,
        B.idade_max_total,
        B.co_mes_min_total,
        B.co_mes_max_total,
        B.co_seq_diretriz_total,
        B.co_seq_cidpri_total,
        B.co_seq_procedimento_total,
        B.co_seq_gestao_total,
        B.co_cnes_estabelecimento_total,
        B.co_seq_municipio_residencia_total,
        B.vl_aprovado_total,
        B.qt_esquema1_total,
        B.qt_esquema2_total         
 from subset A
 left join bd_sabeis.vw_siasus_pa_diretriz_usuario_sintese B
 on A.usuariosus=B.usuariosus;
" | PGPASSWORD=$pw psql -U $us -h $hs -d $dbase -p $pt -e

done



