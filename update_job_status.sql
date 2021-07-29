DELIMITER $$

CREATE PROCEDURE update_job_status(IN  p_job_id VARCHAR(255),IN p_status VARCHAR(255))

BEGIN
	delete from job_status_tb where job_id = p_job_id;
	insert into job_status_tb  values (p_job_id,p_status,sysdate());
        commit;

END$$

DELIMITER ;
