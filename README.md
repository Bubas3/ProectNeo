/*Процедура расчета витрины оборотов*/
CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
BEGIN
    /*Удаляем записи за дату расчета*/
    DELETE FROM DM.DM_ACCOUNT_TURNOVER_F WHERE on_date = i_OnDate;

    /*Вставка новых записей*/
    INSERT INTO DM.DM_ACCOUNT_TURNOVER_F (on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
    SELECT
        i_OnDate,
        ft.credit_account_rk AS account_rk,
        SUM(ft.credit_amount) AS credit_amount,
        SUM(ft.credit_amount * COALESCE(er.reduced_cource, 1)) AS credit_amount_rub,
        SUM(ft.debet_amount) AS debet_amount,
        SUM(ft.debet_amount * COALESCE(er.reduced_cource, 1)) AS debet_amount_rub
    FROM DS.FT_POSTING_F ft
    LEFT JOIN DS.MD_EXCHANGE_RATE_D er
        ON ft.oper_date = er.data_actual_date
    WHERE ft.oper_date = i_OnDate
    GROUP BY ft.credit_account_rk;
Exception
	WHEN OTHERS THEN 
	INSERT INTO logs.etl_log (status,message,start_time,end_time)
	VALUES ('ERROR','Error occurred while processing DM_ACCOUNT_TURNOVER_F for ' || i_OnDate || ': ' || SQLERRM,NOW(),NOW());
END;
$$;

/*Вызов процедуры расчета витрины оборотов*/
DO $$
DECLARE
i_date DATE;
start_time TIMESTAMP;
end_time TIMESTAMP;
BEGIN
	start_time := clock_timestamp();
    FOR i_date IN SELECT generate_series('2018-01-01'::DATE, '2018-01-31'::DATE, INTERVAL '1 day')
    LOOP
        CALL ds.fill_account_turnover_f(i_date);
    END LOOP;
	end_time :=clock_timestamp();
/*Запись логов процедуры*/
	INSERT INTO logs.etl_log (status, message, start_time, end_time)
    VALUES ('SUCCESS', 'Calculated DM_ACCOUNT_TURNOVER_F', start_time, end_time);
END $$;

select * from logs.etl_log


/*Процедура расчета витрины остатков*/
CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
BEGIN
    /*удаление записи за дату расчета*/
    DELETE FROM DM.DM_ACCOUNT_BALANCE_F WHERE on_date = i_OnDate;

    /*вставка новой записи*/
    INSERT INTO DM.DM_ACCOUNT_BALANCE_F (on_date,account_rk, balance_out, balance_out_rub)
    SELECT 
        i_OnDate,
        account_rk,
        CASE 
            WHEN char_type = 'А' THEN COALESCE((SELECT balance_out FROM DM.DM_ACCOUNT_BALANCE_F WHERE on_date = i_OnDate - INTERVAL '1 day' AND account_rk = a.account_rk), 0) 
                + COALESCE((SELECT SUM(debet_amount) FROM DM.DM_ACCOUNT_TURNOVER_F WHERE on_date = i_OnDate AND account_rk = a.account_rk), 0)
                - COALESCE((SELECT SUM(credit_amount) FROM DM.DM_ACCOUNT_TURNOVER_F WHERE on_date = i_OnDate AND account_rk = a.account_rk), 0)
            WHEN char_type = 'П' THEN COALESCE((SELECT balance_out FROM DM.DM_ACCOUNT_BALANCE_F WHERE on_date = i_OnDate - INTERVAL '1 day' AND account_rk = a.account_rk), 0) 
                - COALESCE((SELECT SUM(debet_amount) FROM DM.DM_ACCOUNT_TURNOVER_F WHERE on_date = i_OnDate AND account_rk = a.account_rk), 0)
                + COALESCE((SELECT SUM(credit_amount) FROM DM.DM_ACCOUNT_TURNOVER_F WHERE on_date = i_OnDate AND account_rk = a.account_rk), 0)
        END AS balance_out,
        CASE 
            WHEN char_type = 'А' THEN COALESCE((SELECT balance_out_rub FROM DM.DM_ACCOUNT_BALANCE_F WHERE on_date = i_OnDate - INTERVAL '1 day' AND account_rk = a.account_rk), 0) 
                + COALESCE((SELECT SUM(debet_amount_rub) FROM DM.DM_ACCOUNT_TURNOVER_F WHERE on_date = i_OnDate AND account_rk = a.account_rk), 0)
                - COALESCE((SELECT SUM(credit_amount_rub) FROM DM.DM_ACCOUNT_TURNOVER_F WHERE on_date = i_OnDate AND account_rk = a.account_rk), 0)
            WHEN char_type = 'П' THEN COALESCE((SELECT balance_out_rub FROM DM.DM_ACCOUNT_BALANCE_F WHERE on_date = i_OnDate - INTERVAL '1 day' AND account_rk = a.account_rk), 0) 
                - COALESCE((SELECT SUM(debet_amount_rub) FROM DM.DM_ACCOUNT_TURNOVER_F WHERE on_date = i_OnDate AND account_rk = a.account_rk), 0)
                + COALESCE((SELECT SUM(credit_amount_rub) FROM DM.DM_ACCOUNT_TURNOVER_F WHERE on_date = i_OnDate AND account_rk = a.account_rk), 0)
        END AS balance_out_rub
    FROM 
        DS.MD_ACCOUNT_D a
	WHERE 
        i_OnDate >= a.data_actual_date 
        AND (a.data_actual_end_date IS NULL OR i_OnDate <= a.data_actual_end_date);
END;
$$;

/*Расчет за январь*/
DO $$
DECLARE
	start_time TIMESTAMP;
	end_time TIMESTAMP;
    start_date DATE := '2018-01-01';
    end_date DATE := '2018-01-31';
    cur_date DATE := start_date;
BEGIN
	start_time := clock_timestamp();
    WHILE cur_date <= end_date LOOP
        CALL ds.fill_account_balance_f(cur_date);
        cur_date := cur_date + INTERVAL '1 day';
    END LOOP;
	end_time := clock_timestamp();
	INSERT INTO logs.etl_log (status, message, start_time, end_time)
    VALUES ('SUCCESS', 'Calculated DM_ACCOUNT_BALANCE_F', start_time, end_time);
END $$;

select * from logs.etl_log
SELECT * FROM  DM.DM_ACCOUNT_BALANCE_F

/* Процедура заполнения 101 формы*/
CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_FromDate DATE;
    v_ToDate DATE;
BEGIN
    -- Удаление записей за дату расчета
    DELETE FROM DM.DM_F101_ROUND_F WHERE FROM_DATE = i_OnDate - INTERVAL '1 month';

    -- Установка периода расчета
    v_FromDate := DATE_TRUNC('month', i_OnDate - INTERVAL '1 month');
    v_ToDate := i_OnDate - INTERVAL '1 day';

    -- Расчет данных и вставка в витрину
    INSERT INTO DM.DM_F101_ROUND_F (
        FROM_DATE, TO_DATE, CHAPTER, LEDGER_ACCOUNT, CHARACTERISTIC,
        BALANCE_IN_RUB, BALANCE_IN_VAL, BALANCE_IN_TOTAL,
        TURN_DEB_RUB, TURN_DEB_VAL, TURN_DEB_TOTAL,
        TURN_CRE_RUB, TURN_CRE_VAL, TURN_CRE_TOTAL,
        BALANCE_OUT_RUB, BALANCE_OUT_VAL, BALANCE_OUT_TOTAL
    )
    SELECT
        v_FromDate, v_ToDate, 
        la.chapter, 
        SUBSTRING(CAST(a.account_rk AS VARCHAR) FROM 1 FOR 5) AS ledger_account, 
        a.char_type AS characteristic,
        COALESCE(SUM(CASE WHEN a.currency_code IN ('810', '643') THEN b.balance_out_rub ELSE 0 END), 0) AS balance_in_rub,
        COALESCE(SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN b.balance_out_rub ELSE 0 END), 0) AS balance_in_val,
        COALESCE(SUM(b.balance_out_rub), 0) AS balance_in_total,
        COALESCE(SUM(CASE WHEN a.currency_code IN ('810', '643') THEN t.debet_amount_rub ELSE 0 END), 0) AS turn_deb_rub,
        COALESCE(SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN t.debet_amount_rub ELSE 0 END), 0) AS turn_deb_val,
        COALESCE(SUM(t.debet_amount_rub), 0) AS turn_deb_total,
        COALESCE(SUM(CASE WHEN a.currency_code IN ('810', '643') THEN t.credit_amount_rub ELSE 0 END), 0) AS turn_cre_rub,
        COALESCE(SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN t.credit_amount_rub ELSE 0 END), 0) AS turn_cre_val,
        COALESCE(SUM(t.credit_amount_rub), 0) AS turn_cre_total,
        COALESCE(SUM(CASE WHEN a.currency_code IN ('810', '643') THEN b.balance_out_rub ELSE 0 END), 0) AS balance_out_rub,
        COALESCE(SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN b.balance_out_rub ELSE 0 END), 0) AS balance_out_val,
        COALESCE(SUM(b.balance_out_rub), 0) AS balance_out_total
    FROM 
        DS.MD_ACCOUNT_D a
    LEFT JOIN 
        DS.MD_LEDGER_ACCOUNT_S la ON a.account_rk = la.ledger_account
    LEFT JOIN 
        DM.DM_ACCOUNT_BALANCE_F b ON a.account_rk = b.account_rk AND b.on_date = v_FromDate - INTERVAL '1 day'
    LEFT JOIN 
        DM.DM_ACCOUNT_TURNOVER_F t ON a.account_rk = t.account_rk AND t.on_date BETWEEN v_FromDate AND v_ToDate
    GROUP BY 
        la.chapter, SUBSTRING(CAST(a.account_rk AS VARCHAR) FROM 1 FOR 5), a.char_type;
END;
$$;


DO $$
DECLARE
	start_time TIMESTAMP;
	end_time TIMESTAMP;
BEGIN
	start_time := clock_timestamp();
	CALL dm.fill_f101_round_f('2018-02-01');
	end_time := clock_timestamp();
	INSERT INTO logs.etl_log (status, message, start_time, end_time)
    VALUES ('SUCCESS', 'Calculated DM_F101_ROUND_F', start_time, end_time);
END $$;
