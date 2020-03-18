#!/usr/bin/env bash

CDRCONNECT=jdbc:oracle:thin:@som-racload03:1521/AUTOMTN
CDRUSER=hadoop_mercury
CDRPWD=XU91abLt10aDMb
CDRSPLITBY=FILE_ID

FASTTRACKCONNECT=jdbc:oracle:thin:@som-racload03.humedica.net:1521/AUTOMTN
FASTTRACKUSER=fasttrack
FASTTRACKPWD=Fs9056azePRN1zcwq05
FASTTRACKSPLITBY=FILE_ID


SCHEMA=$1
DEST=RXORDER_TABLES


./acquireRxOrderTable.sh ORDERS H328218_CR2_GENESIS $DEST $CDRCONNECT $CDRUSER $CDRPWD $CDRSPLITBY
./acquireRxOrderTable.sh ENCOUNTER H328218_CR2_GENESIS $DEST $CDRCONNECT $CDRUSER $CDRPWD $CDRSPLITBY
./acquireRxOrderTable.sh ENC_ALIAS H328218_CR2_GENESIS $DEST $CDRCONNECT $CDRUSER $CDRPWD $CDRSPLITBY
./acquireRxOrderTable.sh HUM_DRG H328218_CR2_GENESIS $DEST $CDRCONNECT $CDRUSER $CDRPWD $CDRSPLITBY
./acquireRxOrderTable.sh ORDER_ACTION H328218_CR2_GENESIS $DEST $CDRCONNECT $CDRUSER $CDRPWD $CDRSPLITBY
./acquireRxOrderTable.sh ORDER_DETAIL H328218_CR2_GENESIS $DEST $CDRCONNECT $CDRUSER $CDRPWD $CDRSPLITBY
./acquireRxOrderTable.sh PERSON H328218_CR2_GENESIS $DEST $CDRCONNECT $CDRUSER $CDRPWD $CDRSPLITBY
./acquireRxOrderTable.sh PERSON_ALIAS H328218_CR2_GENESIS $DEST $CDRCONNECT $CDRUSER $CDRPWD $CDRSPLITBY
./acquireRxOrderTable.sh ZH_CODE_VALUE H328218_CR2_GENESIS $DEST $CDRCONNECT $CDRUSER $CDRPWD $CDRSPLITBY
./acquireRxOrderTable.sh ZH_MED_IDENTIFIER H328218_CR2_GENESIS $DEST $CDRCONNECT $CDRUSER $CDRPWD $CDRSPLITBY
./acquireRxOrderTable.sh ZH_NOMENCLATURE H328218_CR2_GENESIS $DEST $CDRCONNECT $CDRUSER $CDRPWD $CDRSPLITBY
./acquireRxOrderTable.sh ZH_ORDER_CATALOG_ITEM_R H328218_CR2_GENESIS $DEST $CDRCONNECT $CDRUSER $CDRPWD $CDRSPLITBY



#H328218_CR2_GENESIS
#H328218_CR2_GRHS
#H416989_CR2_V1
#H565676_CR2_V1
#H667594_CR2_V1
#H729838_CR2_V1
#H984164_CR2
#H984186_CR2_V1
#H984197_CR2_V1
#H984442_CR2_V1
#H984531_CR2_V1
#H984926_CR2_MIDET_V1
#H984926_CR2_MIKAL
#H984945_CR2
#H984993_CR2_V1