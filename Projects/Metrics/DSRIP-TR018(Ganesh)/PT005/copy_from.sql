prompt Copying &TABLE data from &1

call xl.begin_action('Copying &TABLE data from &1');

COPY FROM kollurug/Password1#@&1.DW01 APPEND &TABLE USING -
SELECT * FROM &TABLE;

call xl.end_action();