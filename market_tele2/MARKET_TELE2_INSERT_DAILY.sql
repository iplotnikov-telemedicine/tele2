REPLACE PROCEDURE UAT_PRODUCT.MARKET_TELE2_INSERT_DAILY ()
--call UAT_PRODUCT.MARKET_TELE2_INSERT_DAILY()
--drop procedure UAT_PRODUCT.MARKET_TELE2_INSERT_WEEKLY
--sel distinct report_date, period from UAT_PRODUCT.MARKET_TELE2 order by 2,1

SQL SECURITY INVOKER

BEGIN

	DECLARE days_from DATE;
	DECLARE weeks_from DATE;
	DECLARE months_from DATE;
	DECLARE max_date DATE;
	
	--max_date
	SELECT max_report_date INTO max_date
	FROM PRD2_TMD_V.RDS_LOAD_STATUS
	WHERE Lower(Database_Name) IN ('prd_rds')
		AND Lower(Table_Name) IN ('market_tele2');
	
	--days_from
	SELECT  max_date - 30 INTO days_from;
	DELETE FROM UAT_PRODUCT.MARKET_TELE2 WHERE period = 'daily';
	CALL UAT_PRODUCT.MARKET_TELE2_INSERT ('daily', days_from, max_date);
	
	--weeks_from
	SELECT trunc(max_date - interval '7' day * 20,'IW') INTO weeks_from;
	DELETE FROM UAT_PRODUCT.MARKET_TELE2 WHERE period = 'weekly';
	CALL UAT_PRODUCT.MARKET_TELE2_INSERT ('weekly', weeks_from, trunc(max_date,'IW')-1);
	
	--months_from
	SET months_from = Date'2019-09-01';
	DELETE FROM UAT_PRODUCT.MARKET_TELE2 WHERE period = 'monthly';
	CALL UAT_PRODUCT.MARKET_TELE2_INSERT ('monthly', months_from, trunc(max_date,'mon')-1);

/*	IF till_date >= from_date THEN
		BEGIN		
		CALL UAT_PRODUCT.MARKET_TELE2_INSERT (from_date, till_date);
		END;
	END IF;*/
END;