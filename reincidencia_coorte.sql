drop materialized view if exists tf_siasus_pa_018_usuario_sintese ;
create materialized view tf_siasus_pa_018_usuario_sintese as
select nu_cnspcn usuariosus,
       left(string_agg(sg_sexo,''),1) sexo,
       min(nu_idade) idade_min,
       max(nu_idade) idade_max,
       min(co_seq_competencia) co_mes_min,
       max(co_seq_competencia) co_mes_max,
       string_agg(co_seq_diretriz::text,', ') as co_seq_diretriz,
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
                             from bd_sabeis_diretriz.tf_siasus_pa_018 A
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
                             from bd_sabeis_diretriz.tf_siasus_pa_018 A
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
                             from bd_sabeis_diretriz.tf_siasus_pa_018 A
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
                             from bd_sabeis_diretriz.tf_siasus_pa_018 A
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
                             from bd_sabeis_diretriz.tf_siasus_pa_018 A
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
                             from bd_sabeis_diretriz.tf_siasus_pa_018 A
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
  from bd_sabeis_diretriz.tf_siasus_pa_018
 group by 1;


drop materialized view if exists tf_siasus_pa_018_usuario_sintese ;
create materialized view tf_siasus_pa_018_usuario_sintese as
select nu_cnspcn usuariosus,
       left(string_agg(sg_sexo,''),1) sexo,
       min(nu_idade) idade_min,
       max(nu_idade) idade_max,
       min(co_seq_competencia) co_mes_min,
       max(co_seq_competencia) co_mes_max,
       string_agg(distinct ds_esquema_1,', ') qt_esquema1,
       string_agg(distinct ds_esquema_2,', ') qt_esquema2,
       string_agg(distinct co_seq_procedimento::text,', ') co_seq_procedimento,
       string_agg(distinct co_seq_gestao::text,', ') co_seq_gestao,
       string_agg(distinct co_cnes_estabelecimento::text,', ') co_cnes_estabelecimento,
       string_agg(distinct co_seq_municipio_residencia::text,', ') co_seq_municipio_residencia,
       string_agg(distinct co_seq_cidpri::text,', ') co_seq_cidpri,
       sum(vl_aprovado) vl_aprovado,       
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
                             from bd_sabeis_diretriz.tf_siasus_pa_018 A
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
                             from bd_sabeis_diretriz.tf_siasus_pa_018 A
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
                             from bd_sabeis_diretriz.tf_siasus_pa_018 A
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
                             from bd_sabeis_diretriz.tf_siasus_pa_018 A
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
                             from bd_sabeis_diretriz.tf_siasus_pa_018 A
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
                             from bd_sabeis_diretriz.tf_siasus_pa_018 A
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
  from bd_sabeis_diretriz.tf_siasus_pa_018
 group by 1;

