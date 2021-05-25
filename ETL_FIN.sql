-- ЗАГРУЖАЕМ В МЕТУ ПЕРВУЮ ДАТУ, ЕСЛИ МЕТА НЕ ЗАПОЛНЕНА

insert into META_INCREMENT
 select 'CHUVATKIN', 'STG_INCREMENT', to_date('01.01.1800', 'dd.mm.yyyy') from dual
 where not exists
 (
 select 1 from meta_increment
 where db_name='CHUVATKIN' and tbl_name='STG_INCREMENT');
 
-- ЧИСТИМ СТЕЙДЖИНГ

delete stg_increment;

-- ЗАГРУЖАЕМ ДАННЫЕ В STG_INCREMENT
insert into stg_increment
  select trim(trans_id)
  , to_date(trans_date, 'dd.mm.yyyy hh24:mi:ss') as trans_date
  , trim(card)
  , trim(account)
  , to_char (to_date(account_valid_to, 'mm.dd.yyyy'), 'dd.mm.yyyy') as account_valid_to
  , trim(client)
  , trim(last_name)
  , trim(first_name)
  , trim(patronymic)
  , to_char(to_date(date_of_birth, 'mm.dd.yyyy'),'dd.mm.yyyy') date_of_birth
  , trim(passport)
  , to_char(to_date(passport_valid_to, 'mm.dd.yyyy'),'dd.mm.yyyy') passport_valid_to
  , REGEXP_REPLACE(SUBSTR(REGEXP_REPLACE(phone, '[^0-9]', ''), 2,20), '^(\D*)', '+7') phone
  , trim(oper_type)
  , cast(amount as decimal(10,2))
  , trim(oper_result)
  , trim(terminal)
  , trim(terminal_type)
  , trim(city)
  , trim(address)
  from source_transactions
  where trans_date >(
                select last_dt_transaction from meta_increment
                where db_name = 'CHUVATKIN'
                and tbl_name = 'STG_INCREMENT'
                );    
          

-- MERGE ДЛЯ ЗАКРЫТИЯ ПРЕДЫДУЩЕЙ ВЕРСИИ И ЗАГРУЗКИ ТЕХ ЗАПИСЕЙ КОТОРЫХ НЕТ В ХРАНИЛИЩЕ dim_terminals_hist

merge into dim_terminals_hist dth
using (
  select terminal, terminal_type, city, address, terminal_start_dt 
  from (select stg1.terminal
              , stg1.terminal_type
              , stg1.city
              , stg1.address
              , to_date(stg1.trans_date, 'dd.mm.yyyy hh24:mi:ss') as terminal_start_dt
              , first_value(stg1.trans_date) over (partition by stg1.terminal order by stg1.trans_date range between unbounded preceding and unbounded following) as first_terminal
        from stg_increment stg1
        left join dim_terminals_hist dth1
          on stg1.terminal=dth1.terminal_id
        where dth1.terminal_id is null or stg1.address <> dth1.terminal_address or stg1.terminal_type <> dth1.terminal_type
        )
  where terminal_start_dt = first_terminal
       ) stg
on (dth.terminal_id=stg.terminal and (stg.address <> dth.terminal_address or stg.terminal_type <> dth.terminal_type))
when matched 
then update set 
    dth.terminal_end_dt=stg.terminal_start_dt
    where dth.terminal_end_dt='31.12.2999'
when not matched 
then insert (
    dth.terminal_id
    , dth.terminal_type
    , dth.terminal_city
    , dth.terminal_address
    , dth.terminal_start_dt
    , dth.terminal_end_dt
)values(
    stg.terminal
    , stg.terminal_type
    , stg.city
    , stg.address
    , stg.terminal_start_dt
    , to_date('31.12.2999', 'DD.MM.YYYY')
);

-- INSERT НОВОЙ ВЕРСИИ ЗАПИСИ ДЛЯ dim_terminals_hist
insert into dim_terminals_hist (
  terminal_id
  , terminal_type
  , terminal_city
  , terminal_address
  , terminal_start_dt
  , terminal_end_dt
  ) select 
  terminal
  , stg.terminal_type
  , stg.city
  , stg.address
  , to_date(stg.trans_date) as terminal_start_dt
  , to_date('31.12.2999', 'DD.MM.YYYY HH24:MI:SS') as terminal_end_dt
  from stg_increment stg
  left join dim_terminals_hist dth
  on stg.terminal=dth.terminal_id
  where stg.trans_date = dth.terminal_end_dt;

-- MERGE ДЛЯ ТАБЛИЦЫ КЛИЕНТОВ SCD2, ВЕРСИОННОСТЬ ПРОВЕРЯЕТСЯ ПО ВСЕМ ПОЛЯМ

merge into dim_clients_hist dch
using (
  select client
        , last_name
        , first_name
        , patronymic
        , date_of_birth
        , passport
        , passport_valid_to
        , phone
        , client_start_dt 
  from(select stg1.client
             , stg1.last_name
             , stg1.first_name
             , stg1.patronymic
             , stg1.date_of_birth
             , stg1.passport
             , stg1.passport_valid_to
             , stg1.phone
             , stg1.trans_date as client_start_dt 
             , first_value (stg1.trans_date) over (partition by stg1.client order by stg1.trans_date range between unbounded preceding and unbounded following) as first_client
       from stg_increment stg1
       left join dim_clients_hist dch1
         on stg1.client=dch1.client_id
       where dch1.client_id is null or stg1.last_name<>dch1.last_name 
       or stg1.first_name<>dch1.first_name 
       or stg1.patronymic<>dch1.patronymic 
       or stg1.passport <> dch1.passport_num 
       or stg1.passport_valid_to<>dch1.passport_valid_to
       or stg1.phone<>dch1.phone
       )
  where client_start_dt = first_client
       ) stg
on (dch.client_id=stg.client and (stg.last_name<>dch.last_name or stg.first_name<>dch.first_name 
    or stg.patronymic<>dch.patronymic or stg.passport <> dch.passport_num 
    or stg.passport_valid_to<>dch.passport_valid_to or stg.phone<>dch.phone)
    )
when matched 
then update set 
  dch.client_end_dt=stg.client_start_dt
  where dch.client_end_dt='31.12.2999'
when not matched 
 then insert (
  dch.client_id
  , dch.last_name
  , dch.first_name
  , dch.patronymic
  , dch.date_of_birth
  , dch.passport_num
  , dch.passport_valid_to
  , dch.phone
  , dch.client_start_dt
  , dch.client_end_dt
)values(
  stg.client
  , stg.last_name
  , stg.first_name
  , stg.patronymic
  , stg.date_of_birth
  , stg.passport
  , stg.passport_valid_to
  , stg.phone
  , stg.client_start_dt
  , to_date('31.12.2999', 'DD.MM.YYYY')
);
select * from dim_clients_hist
-- INSERT НОВОЙ ВЕРСИИ ЗАПИСИ КЛИЕНТОВ
insert into dim_clients_hist (
  client_id
  , last_name
  , first_name
  , patronymic
  , date_of_birth
  , passport_num
  , passport_valid_to
  , phone
  , client_start_dt
  , client_end_dt
) select 
  stg.client
  , stg.last_name
  , stg.first_name
  , stg.patronymic
  , stg.date_of_birth
  , stg.passport
  , stg.passport_valid_to
  , stg.phone
  , to_date(stg.trans_date, 'DD.MM.YYYY HH24:MI:SS') as client_start_dt
  , to_date('31.12.2999', 'dd.mm.yyyy') as client_end_dt
  from stg_increment stg
  left join dim_clients_hist dch
  on stg.client=dch.client_id 
  where stg.trans_date = dch.client_end_dt;

-- MERGE ДЛЯ ТАБЛИЦЫ АККАУНТ - (СМЕНА ВЛАДЕЛЬЦА СЧЕТА ИЛИ ИЗМЕНЕНИЕ ДАТЫ ОКОНЧАНИЯ СЧЕТА) 

merge into dim_accounts_hist dah
using (
  select account
       , account_valid_to
       , client
       , account_start_dt 
  from (
        select stg1.account
             , stg1.account_valid_to
             , stg1.client
             , stg1.trans_date as account_start_dt
             , first_value (stg1.trans_date) over (partition by stg1.account order by stg1.trans_date range between unbounded preceding and unbounded following) as first_account
        from stg_increment stg1
        left join dim_accounts_hist dah1
          on stg1.account=dah1.account_num
        where dah1.account_num is null or stg1.client <> dah1.client or stg1.account_valid_to <> dah1.valid_to
        )
  where account_start_dt = first_account
      ) stg
on (dah.account_num=stg.account and (stg.client <> dah.client or stg.account_valid_to <> dah.valid_to))
when matched 
then update set 
    dah.account_end_dt=stg.account_start_dt
    where dah.account_start_dt='31.12.2999'
when not matched 
then insert (
  dah.account_num
  , dah.valid_to
  , dah.client
  , dah.account_start_dt
  , dah.account_end_dt
  )values(
  stg.account
  , stg.account_valid_to
  , stg.client
  , stg.account_start_dt
  , to_date('31.12.2999', 'DD.MM.YYYY')
);

-- INSERT НОВОЙ ВЕРСИИ ЗАПИСИ ACCOUNTS

insert into dim_accounts_hist (
  account_num
  , valid_to
  , client
  , account_start_dt
  , account_end_dt
  ) select
  stg.account
  , stg.account_valid_to
  , stg.client
  , to_date(stg.trans_date, 'DD.MM.YYYY HH24:MI:SS') as account_start_dt
  , to_date('31.12.2999', 'dd.mm.yyyy') as account_end_dt
  from stg_increment stg
  left join dim_accounts_hist dah
  on stg.account=dah.account_num
  where stg.trans_date = dah.account_end_dt;


-- MERGE ДЛЯ CARDS ЕСЛИ У СЧЕТА ПОМЕНЯЛСЯ НОМЕР КАРТЫ

merge into dim_cards_hist dh
using (
  select card
        , account
        , card_start_dt 
  from(select stg1.card
              , stg1.account
              , stg1.trans_date  as card_start_dt
              , first_value (stg1.trans_date) over (partition by stg1.card order by stg1.trans_date range between unbounded preceding and unbounded following) as first_card
       from stg_increment stg1
       left join dim_cards_hist dh1
         on stg1.card = dh1.card_num
       where dh1.card_num is null
       )
  where card_start_dt = first_card
        ) stg
on (dh.account_num=stg.account)
when matched then update set 
  dh.card_end_dt=stg.card_start_dt
  where dh.card_end_dt='31.12.2999'
when not matched 
then insert (
  dh.card_num
  , dh.account_num
  , dh.card_start_dt
  , dh.card_end_dt
)values(
  stg.card
  , stg.account
  , stg.card_start_dt
  , to_date('31.12.2999', 'DD.MM.YYYY')
);

-- ЗАГРУЗКА ВЕРСИОННЫХ ЗАПИСЕЙ ДЛЯ dim_cards_hist

insert into dim_cards_hist (
  card_num
  , account_num
  , card_start_dt
  , card_end_dt
  ) select
  stg.card
  , stg.account
  , to_date(stg.trans_date, 'DD.MM.YYYY HH24:MI:SS') as card_start_dt
  , to_date('31.12.2999', 'dd.mm.yyyy') as card_end_dt
  from stg_increment stg
  left join dim_cards_hist dh
    on stg.account=dh.account_num
  where stg.trans_date = dh.card_end_dt;

-- ЗАГРУЖАЕМ ДАННЫЕ В ТАБЛИЦУ ФАКТ_ТРАНЗАКЦИИ
insert into fact_transactions ( 
  trans_id
  , trans_date
  , card_num
  , oper_type
  , amt
  , oper_result
  , terminal
  ) select 
  trans_id
  , trans_date
  , card
  , oper_type
  , amount
  , oper_result
  , terminal
from stg_increment;

-- ОБНОВЛЯЕМ meta_increment
update meta_increment set last_dt_transaction = (select max(trans_date) from stg_increment);

commit;
