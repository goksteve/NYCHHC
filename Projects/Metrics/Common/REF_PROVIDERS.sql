drop table ref_providers purge;

CREATE TABLE REF_PROVIDERS
(
  PROVIDER_ID               NUMBER(12)          NOT NULL,
  PROVIDER_NAME             VARCHAR2(60 BYTE),
  TITLE_ID                  NUMBER(12),
  TITLE_NAME                VARCHAR2(100 BYTE),
  TITLE_PREFIX              VARCHAR2(20 BYTE),
  TITLE_SUFFIX              VARCHAR2(50 BYTE),
  PHYSICIAN_FLAG            VARCHAR2(5 BYTE),
  EMP_ID                    NUMBER(12),
  EMP_VALUE                 VARCHAR2(2048 BYTE),
  LICENSE_ID                NUMBER(12),
  LICENSE_VALUE             VARCHAR2(2048 BYTE),
  SOCIAL_SECURITY_ID        NUMBER(12),
  SOCIAL_SECURITY_VALUE     VARCHAR2(2048 BYTE),
  SDG_EMP_NO_ID             NUMBER(12),
  SDG_EMP_NO_VALUE          VARCHAR2(2048 BYTE),
  PRAC_NPI_ID               NUMBER(12),
  PRAC_NPI_VALUE            VARCHAR2(2048 BYTE),
  NPI_ID                    NUMBER(12),
  NPI_VALUE                 VARCHAR2(2048 BYTE),
  LICENSE_EXP_DATE_ID       NUMBER(12),
  LICENSE_EXP_DATE_VALUE    VARCHAR2(2048 BYTE),
  PHYSICIAN_SERVICE_ID      VARCHAR2(4000 BYTE),
  PHYSICIAN_SERVICE_NAME    VARCHAR2(4000 BYTE),
  PHYSICIAN_SERVICE_ID_1    VARCHAR2(4000 BYTE),
  PHYSICIAN_SERVICE_NAME_1  VARCHAR2(4000 BYTE),
  PHYSICIAN_SERVICE_ID_2    VARCHAR2(4000 BYTE),
  PHYSICIAN_SERVICE_NAME_2  VARCHAR2(4000 BYTE),
  PHYSICIAN_SERVICE_ID_3    VARCHAR2(4000 BYTE),
  PHYSICIAN_SERVICE_NAME_3  VARCHAR2(4000 BYTE),
  PHYSICIAN_SERVICE_ID_4    VARCHAR2(4000 BYTE),
  PHYSICIAN_SERVICE_NAME_4  VARCHAR2(4000 BYTE),
  PHYSICIAN_SERVICE_ID_5    VARCHAR2(4000 BYTE),
  PHYSICIAN_SERVICE_NAME_5  VARCHAR2(4000 BYTE),
  LOAD_DATE                 DATE,
  SOURCE                    CHAR(4 BYTE),
  CONSTRAINT pk_ref_providers PRIMARY KEY(provider_id)
) COMPRESS BASIC; 