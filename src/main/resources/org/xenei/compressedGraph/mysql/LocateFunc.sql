CREATE FUNCTION `register_node` ( `v_data` BLOB ) RETURN INT
MODIFIES SQL DATA
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE _idx INT;
    DECLARE _data BLOB;
    DECLARE cur1 CURSOR FOR SELECT idx,data FROM spNode WHERE data=v_data;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done=TRUE;

    OPEN cur1;
    
    read_loop: LOOP
        FETCH cur1 INTO _idx, _data;
        IF done THEN
            INSERT INTO spNode (data) VALUE (v_data);
            SELECT LAST_INSERT_ID() INTO _idx;
            LEAVE read_loop;
        END IF;
        IF _data = v_data THEN
            LEAVE read_loop;
        END IF;
    END LOOP;
    
    CLOSE cur1;
    
    RETURN _idx;
END