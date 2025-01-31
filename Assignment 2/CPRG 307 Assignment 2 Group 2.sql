-- Code to process new transactions and update account balances
SET serveroutput on;

DECLARE
    -- Constants for transaction types
    v_credit CONSTANT CHAR(1) := 'C';
    v_debit CONSTANT CHAR(1) := 'D';

    -- Cursor to fetch new transactions
    cursor c_new_trans is
        select * from new_transactions;

    -- Variables to store transaction details
    v_default_trans_type CHAR(1);
    v_account_balance NUMBER;


BEGIN
    -- Loop through new transactions
    FOR trans_rec IN c_new_trans LOOP
        -- Fetch default transaction type for the account
        SELECT at.default_trans_type INTO v_default_trans_type
        FROM account a
        JOIN account_type at ON a.account_type_code = at.account_type_code
        WHERE a.account_no = trans_rec.account_no;

        -- Insert into transaction history
        INSERT INTO transaction_history
            VALUES(trans_rec.transaction_no, trans_rec.transaction_date, trans_rec.description);
            
        -- Insert into transaction_detail
        INSERT INTO transaction_detail
            VALUES(trans_rec.account_no, trans_rec.transaction_no, trans_rec.transaction_type, trans_rec.transaction_amount);

        -- Get current balance
        SELECT account_balance INTO v_account_balance
        FROM account
        WHERE account_no = trans_rec.account_no;

        -- Update account balance based on transaction type
        IF trans_rec.transaction_type = v_default_trans_type THEN
            v_account_balance := v_account_balance + trans_rec.transaction_amount;
        ELSE
            v_account_balance := v_account_balance - trans_rec.transaction_amount;
        END IF;

        -- Update account balance
        UPDATE account
        SET account_balance = v_account_balance
        WHERE account_no = trans_rec.account_no;

    END LOOP;

    -- Delete processed transactions
    DELETE FROM new_transactions WHERE transaction_no IN (SELECT transaction_no FROM transaction_history);

    -- Commit the changes
    COMMIT;
END;
/
