/********************************************************************
*Author: Nathan Garcia, Jubril Somide
*Created: 12/8/23
*Description: This is the code for the trigger that will move the transactions from 
*the new_transactions table to the transaction_history and transaction_detail tables
*
********************************************************************/
SET serveroutput on;
SET LINESIZE 200;
SET PAGESIZE 100;

--Code to move transactions from new_transactions table to transaction_history and transaction_detail tables
DECLARE
    MISSING_TRANSACTION EXCEPTION;
    PRAGMA EXCEPTION_INIT(MISSING_TRANSACTION, -20001);

    UNEQUAL_DEBITS_CREDITS EXCEPTION;
    PRAGMA EXCEPTION_INIT(UNEQUAL_DEBITS_CREDITS, -20002);

    INVALID_ACCOUNT EXCEPTION;
    PRAGMA EXCEPTION_INIT(INVALID_ACCOUNT, -20003);

    NEGATIVE_AMOUNT EXCEPTION;
    PRAGMA EXCEPTION_INIT(NEGATIVE_AMOUNT, -20004);

    INVALID_TRANSACTION_TYPE EXCEPTION;
    PRAGMA EXCEPTION_INIT(INVALID_TRANSACTION_TYPE, -20005);

    NULL_TRANSACTION_VALUES EXCEPTION;
    PRAGMA EXCEPTION_INIT(NULL_TRANSACTION_VALUES, -20006);

    v_credit CONSTANT CHAR(1) := 'C';
    v_debit CONSTANT CHAR(1) := 'D';
    v_error_msg wkis_error_log.error_msg%TYPE;
    v_all_debits NUMBER;
    v_all_credits NUMBER;
    v_count NUMBER;

    --cursor to get unique transactions from new_transactions table
    cursor c_unique_new_trans is
        select DISTINCT transaction_no, transaction_date, description	
        from new_transactions;

    --cursor to get all transactions from new_transactions table
    cursor c_new_trans is
        select * from new_transactions;

BEGIN
    --Loop through the unique transactions and insert them into transaction_history table
    FOR trans_unq_rec IN c_unique_new_trans LOOP
        v_error_msg := NULL;
        BEGIN
            --Log error when the transaction number is null or missing
            IF trans_unq_rec.transaction_no IS NULL THEN
                RAISE MISSING_TRANSACTION;
            END IF;
            --Log error when the transaction date or description is null or missing
            IF trans_unq_rec.transaction_date IS NULL OR trans_unq_rec.description IS NULL THEN
                RAISE NULL_TRANSACTION_VALUES;
            END IF;

            --Populate transaction history table with the unique transactions
            INSERT INTO transaction_history
            VALUES(trans_unq_rec.transaction_no, trans_unq_rec.transaction_date, trans_unq_rec.description);

            --Display the transaction details
            dbms_output.put_line('---------------------------------------------------------------------------------');
            dbms_output.put_line('Transaction number ' || trans_unq_rec.transaction_no || ' has been inserted into transaction_history table');
            dbms_output.put_line('Transaction Details: ');
            dbms_output.put_line('Transaction Number: ' || trans_unq_rec.transaction_no);
            dbms_output.put_line('Transaction Date: ' || trans_unq_rec.transaction_date);
            dbms_output.put_line('Transaction Description: ' || trans_unq_rec.description);
            dbms_output.put_line('---------------------------------------------------------------------------------');
        EXCEPTION
            WHEN MISSING_TRANSACTION THEN
                v_error_msg := 'The transaction number is missing or null';
                INSERT INTO wkis_error_log (transaction_no, transaction_date, description, error_msg)
                VALUES (trans_unq_rec.transaction_no, trans_unq_rec.transaction_date, trans_unq_rec.description, v_error_msg);
                CONTINUE;
            WHEN NULL_TRANSACTION_VALUES THEN
                v_error_msg := 'One or more of the transaction values are null';
                INSERT INTO wkis_error_log (transaction_no, transaction_date, description, error_msg)
                VALUES (trans_unq_rec.transaction_no, trans_unq_rec.transaction_date, trans_unq_rec.description, v_error_msg);
                CONTINUE;
            WHEN OTHERS THEN
                v_error_msg := 'An unexpected error for ' || trans_unq_rec.transaction_no || ' has occured : ' || SQLERRM;
                INSERT INTO wkis_error_log (transaction_no, transaction_date, description, error_msg)
                VALUES (trans_unq_rec.transaction_no, trans_unq_rec.transaction_date, trans_unq_rec.description, v_error_msg);
                CONTINUE;
        END;

    END LOOP; 
    COMMIT;
    --Loop through all the transactions and insert them into transaction_detail table 
    FOR trans_rec IN c_new_trans LOOP
        BEGIN
            v_error_msg := NULL;
            --Log error when the transaction number is null or missing
            IF trans_rec.transaction_no IS NULL THEN
                RAISE MISSING_TRANSACTION;
            END IF;

            --Log error when the transaction date or description is null or missing
            IF trans_rec.transaction_date IS NULL OR trans_rec.description IS NULL THEN
                RAISE NULL_TRANSACTION_VALUES;
            END IF;

            --Log error when the transaction type is not C or D
            IF trans_rec.transaction_type NOT IN (v_credit, v_debit) THEN
                RAISE INVALID_TRANSACTION_TYPE;
            END IF;

            --Log error when the account number is invalid
            SELECT COUNT(*) INTO v_count FROM account WHERE account_no = trans_rec.account_no;
                IF v_count = 0 THEN
                    RAISE INVALID_ACCOUNT;
                END IF;
            
            SELECT SUM(CASE WHEN transaction_type = 'D' THEN transaction_amount ELSE 0 END), SUM(CASE WHEN transaction_type = 'C' THEN transaction_amount ELSE 0 END)
            INTO v_all_debits, v_all_credits
            FROM new_transactions
            WHERE transaction_no = trans_rec.transaction_no;

            --Log error when the debits and credits are not equal
            IF v_all_debits <> v_all_credits THEN
                RAISE UNEQUAL_DEBITS_CREDITS;
            END IF;
            
            --Log error when the transaction amount is negative
            IF trans_rec.transaction_amount < 0 THEN
                RAISE NEGATIVE_AMOUNT;
            END IF;
            
            --Insert the transaction into transaction_detail table
            INSERT INTO transaction_detail
            VALUES(trans_rec.account_no, trans_rec.transaction_no, trans_rec.transaction_type, trans_rec.transaction_amount);

            --Update account information when it is a debit transaction
            IF trans_rec.transaction_type = v_debit THEN
                UPDATE account
                SET account_balance = account_balance + trans_rec.transaction_amount
                WHERE account_no = trans_rec.account_no;

            ELSIF trans_rec.transaction_type = v_credit THEN
                UPDATE account 
                SET account_balance = account_balance - trans_rec.transaction_amount
                WHERE account_no = trans_rec.account_no;
            END IF;

            --Display the transaction details            
            dbms_output.put_line('---------------------------------------------------------------------------------');
            dbms_output.put_line('Transaction number ' || trans_rec.transaction_no || ' has been inserted into transaction_detail table and accounts updated');
            dbms_output.put_line('Transaction Details: ');
            dbms_output.put_line('Transaction Number: ' || trans_rec.transaction_no);
            dbms_output.put_line('Transaction Date: ' || trans_rec.transaction_date);
            dbms_output.put_line('Transaction Description: ' || trans_rec.description);
            dbms_output.put_line('Transaction Amount: ' || trans_rec.transaction_amount);
            dbms_output.put_line('---------------------------------------------------------------------------------');
            
            --Delete the transaction from new_transactions table
            DELETE FROM new_transactions WHERE transaction_no = trans_rec.transaction_no;
        
        EXCEPTION
            WHEN MISSING_TRANSACTION THEN
                v_error_msg := 'The transaction number is missing or null';
                INSERT INTO wkis_error_log (transaction_no, transaction_date, description, error_msg)
                VALUES (trans_rec.transaction_no, trans_rec.transaction_date, trans_rec.description, v_error_msg);
                CONTINUE;
            WHEN NULL_TRANSACTION_VALUES THEN
                v_error_msg := 'One or more of the transaction values are null';
                INSERT INTO wkis_error_log (transaction_no, transaction_date, description, error_msg)
                VALUES (trans_rec.transaction_no, trans_rec.transaction_date, trans_rec.description, v_error_msg);
                CONTINUE;
            WHEN INVALID_TRANSACTION_TYPE THEN
                v_error_msg := 'The transaction type ' || trans_rec.transaction_type || 'is invalid(should be C or D)';
                INSERT INTO wkis_error_log (transaction_no, transaction_date, description, error_msg)
                VALUES (trans_rec.transaction_no, trans_rec.transaction_date, trans_rec.description, v_error_msg);
                CONTINUE;
            WHEN INVALID_ACCOUNT THEN
                v_error_msg := 'The account number : ' || TO_CHAR(trans_rec.account_no) || ' is invalid';
                INSERT INTO wkis_error_log (transaction_no, transaction_date, description, error_msg)
                VALUES (trans_rec.transaction_no, trans_rec.transaction_date, trans_rec.description, v_error_msg);
                CONTINUE;
            WHEN UNEQUAL_DEBITS_CREDITS THEN
                v_error_msg := 'The total debits and credits are not equal or not balanced';
                INSERT INTO wkis_error_log (transaction_no, transaction_date, description, error_msg)
                VALUES (trans_rec.transaction_no, trans_rec.transaction_date, trans_rec.description, v_error_msg);
                CONTINUE;
            WHEN NEGATIVE_AMOUNT THEN
                v_error_msg := 'The transaction amount is negative: ' || TO_CHAR(trans_rec.transaction_amount);
                INSERT INTO wkis_error_log (transaction_no, transaction_date, description, error_msg)
                VALUES (trans_rec.transaction_no, trans_rec.transaction_date, trans_rec.description, v_error_msg);
                CONTINUE;
            WHEN OTHERS THEN
                v_error_msg := 'An unexpected error for ' || trans_rec.transaction_no || ' has occured : ' || SQLERRM;
                INSERT INTO wkis_error_log (transaction_no, transaction_date, description, error_msg)
                VALUES (trans_rec.transaction_no, trans_rec.transaction_date, trans_rec.description, v_error_msg);
                CONTINUE;
        END;

    END LOOP;
    COMMIT;
    --Loops through the accounts and updates it
    FOR account_rec IN (SELECT * FROM account) LOOP
        -- Checks if the data in account is a liablity ot owners equity
        IF (SUBSTR(account_rec.account_no, 0, 1) = '2') OR (SUBSTR(account_rec.account_no, 0, 1) = '5') THEN
            UPDATE ACCOUNT
            SET account_balance = account_balance * -1
            WHERE account_no = account_rec.account_no;
        END IF;

    END LOOP;
    COMMIT;
END;
/
