mails = LOAD '/home/ubuntu/hadoop-dataset/enron_spam_data.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER'); 

mailsParsedDate = FILTER mails BY $4 MATCHES '^\\d{4}\\-(0?[1-9]|1[012])\\-(0?[1-9]|[12][0-9]|3[01])$'; 

mailsHeader = FOREACH mailsParsedDate GENERATE $1 AS Subject, $2 AS Content, ToDate($4, 'yyyy-MM-dd') as (Date:DateTime); 

mailsCleanedF1 = FILTER mailsHeader by GetYear(Date) >= 2000; 

STORE mailsCleanedF1 INTO '/home/ubuntu/hadoop-dataset/output/enrom_clean_dataset' using org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');