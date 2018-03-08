INSERT INTO meta_criteria VALUES(47, 'RESULT_VALUES:EXCLUDE_LIST', 'The list of Result Values to ignore (patterns)');

INSERT INTO meta_conditions VALUES(47, 'ALL', 'NONE', '%na%', 'Pattern to ignore', 'RV', 'LIKE', 'I');
INSERT INTO meta_conditions VALUES(47, 'ALL', 'NONE', '%n/a%', 'Pattern to ignore', 'RV', 'LIKE', 'I');
INSERT INTO meta_conditions VALUES(47, 'ALL', 'NONE', '%not%', 'Pattern to ignore', 'RV', 'LIKE', 'I');
INSERT INTO meta_conditions VALUES(47, 'ALL', 'NONE', '%none%', 'Pattern to ignore', 'RV', 'LIKE', 'I');
INSERT INTO meta_conditions VALUES(47, 'ALL', 'NONE', '%unable%', 'Pattern to ignore', 'RV', 'LIKE', 'I');
INSERT INTO meta_conditions VALUES(47, 'ALL', 'NONE', '%remind%patient%', 'Pattern to ignore', 'RV', 'LIKE', 'I');
INSERT INTO meta_conditions VALUES(47, 'ALL', 'NONE', '%no%record%', 'Pattern to ignore', 'RV', 'LIKE', 'I');
INSERT INTO meta_conditions VALUES(47, 'ALL', 'NONE', '%nn/a%', 'Pattern to ignore', 'RV', 'LIKE', 'I');
INSERT INTO meta_conditions VALUES(47, 'ALL', 'NONE', '%rt arm%', 'Pattern to ignore', 'RV', 'LIKE', 'I');
INSERT INTO meta_conditions VALUES(47, 'ALL', 'NONE', '%rt foot%', 'Pattern to ignore', 'RV', 'LIKE', 'I');

COMMIT;
