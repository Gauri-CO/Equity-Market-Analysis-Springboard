

DROP PROCEDURE IF EXISTS update_batch_dt;

DELIMITER $$


CREATE PROCEDURE update_batch_dt()

BEGIN

    DECLARE curr_dt_val DATE DEFAULT '1900-01-01';
    DECLARE prev_dt_val DATE DEFAULT '1900-01-01';
    DECLARE next_dt DATE DEFAULT '1900-01-01';
    DECLARE weekday INT default 0;
    DECLARE flag INT default 0;
    
    
    SELECT curr_dt into curr_dt_val FROM batch_control_tb where curr_ind=1;
    SELECT prev_dt into prev_dt_val FROM batch_control_tb where curr_ind=1;
    SELECT DATE_ADD(curr_dt_val, INTERVAL 1 DAY) into next_dt ;
    SELECT WEEKDAY(next_dt) into weekday;
    
    WHILE flag = 0 DO
		if weekday = 5 or weekday = 6 then
			SELECT DATE_ADD(next_dt, INTERVAL 1 DAY) into next_dt ;
                        SELECT WEEKDAY(next_dt) into weekday;
			set flag = 0;
		else
			set flag = 1;
		
               end if;
   end WHILE;

   update batch_control_tb
   set curr_ind=0
   where curr_dt = curr_dt_val and prev_dt=prev_dt_val;


  insert into batch_control_tb values(next_dt,curr_dt_val,1,sysdate());
  commit;

 

END$$


DELIMITER ;