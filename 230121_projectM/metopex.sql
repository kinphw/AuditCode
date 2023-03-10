-- 1. DATABASE 생성
CREATE DATABASE met_opex_2021 DEFAULT CHARSET=utf8;

-- TABLE 생성
CREATE TABLE MET_OPEX_2021 (
    `DB_ID` INT AUTO_INCREMENT,
    `Ledger` VARCHAR(12) ,
    `Ledger Name` VARCHAR(25) ,
    `GL Account` VARCHAR(12) ,
    `GL Account Name` VARCHAR(255) ,
    `Account Descr (English)` VARCHAR(255) ,
    `Fiscal Year` INTEGER(4) ,
    `Period` VARCHAR(20) ,
    `Accounting date` DATE ,
    `Posting Date` DATE ,
    `Journal ID` VARCHAR(20) ,
    `Department ID` VARCHAR(20) ,
    `Department ID Name` VARCHAR(255) ,
    `Distribution Channel` VARCHAR(20) ,
    `Distribution Channel Name` VARCHAR(255) ,
    `Operating Unit` VARCHAR(20) ,
    `Operating Unit Name` VARCHAR(255) ,
    `Product` VARCHAR(20) ,
    `Product Name` VARCHAR(255) ,
    `GL Activity` VARCHAR(12) ,
    `GL Activity Name` VARCHAR(255) ,
    `Fund code` VARCHAR(20) ,
    `Fund code Name` VARCHAR(20) ,
    `Transaction Currency` VARCHAR(3) ,
    `Transaction amount` DECIMAL(30,2) ,
    `Exchange rate` DECIMAL(10,2) ,
    `Base Currency` VARCHAR(3) ,
    `Base amount` BIGINT(64) ,
    `Line Description` VARCHAR(255) ,
    `Source code` VARCHAR(5) ,
    `Source code Name` VARCHAR(255) ,
    `Treaty Code` VARCHAR(5) ,
    `Treaty Code Name` VARCHAR(255) ,
    `Affilate Business Unit` VARCHAR(5) ,
    `Affilate Business Unit Name` VARCHAR(255) ,
    `Origin Code ( Source code)` VARCHAR(10) ,
    `User ID (Submitter's ID)` VARCHAR(10) ,
    `User ID Name` VARCHAR(255) ,
    `Journal Class` VARCHAR(255) ,
    `Journal Class Name` VARCHAR(255) ,
    `Affilate Operating Unit` VARCHAR(20) ,
    `Affilate Operating Unit Name` VARCHAR(255) ,
    `Affilate Fund` VARCHAR(20) ,
    `Affilate Fund Name` VARCHAR(255) ,
    `ML Admin Batch ID` VARCHAR(255) ,
    `Reference` VARCHAR(255) ,
    `Legacy Account` VARCHAR(255) ,
    `Original Acct` VARCHAR(255) ,
    `Voucher Number` VARCHAR(255) ,
    `Trans Code` VARCHAR(255) ,
    `Group Number` VARCHAR(255) ,
        PRIMARY KEY(DB_ID)
);

-- Column명 변경

ALTER TABLE MET_OPEX_2021 CHANGE `GL_Account` `GL Account` VARCHAR(12);

-- 선택
SELECT * FROM MET_OPEX_2021 LIMIT 100;

-- 테이블구조 복사
CREATE TABLE IF NOT EXISTS MET_OPEX_2022 LIKE MET_OPEX_2021;

-- 테이블구조 복사
CREATE TABLE IF NOT EXISTS tgtcoa_2022 LIKE tgtcoa;

-- 인덱스 설정
ALTER TABLE tgtcoa_2022 ADD INDEX i1(COA);

ALTER TABLE met_opex_2022 ADD INDEX i1(`GL Account`);