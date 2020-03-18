package com.humedica.mercury.etl.crossix.rxorder

import com.humedica.mercury.etl.core.engine.EntitySource
import org.apache.spark.sql.DataFrame

/**
  * Created by bhenriksen on 5/17/17.
  */
class RxorderBackendsethgpids(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

    columns = List( "DATASRC", "FACILITYID", "RXID", "ENCOUNTERID", "PATIENTID", "LOCALMEDCODE", "LOCALDESCRIPTION", "LOCALPROVIDERID", "ISSUEDATE",
        "DISCONTINUEDATE", "LOCALSTRENGTHPERDOSEUNIT", "LOCALSTRENGTHUNIT", "LOCALROUTE", "EXPIREDATE", "QUANTITYPERFILL", "FILLNUM", "SIGNATURE", "ORDERTYPE", "ORDERSTATUS",
        "ALTMEDCODE", "MAPPEDNDC", "MAPPEDGPI", "MAPPEDNDC_CONF", "MAPPEDGPI_CONF", "VENUE", "MAP_USED", "HUM_MED_KEY", "LOCALDAYSUPPLIED", "LOCALFORM", "LOCALQTYOFDOSEUNIT",
        "LOCALTOTALDOSE", "LOCALDOSEFREQ", "LOCALDURATION", "LOCALINFUSIONRATE", "LOCALINFUSIONVOLUME", "LOCALINFUSIONDURATION", "LOCALNDC", "NDC11", "LOCALGPI",
        "LOCALDOSEUNIT", "LOCALDISCHARGEMEDFLG",  "LOCALDAW", "ORDERVSPRESCRIPTION", "HUM_GEN_MED_KEY", "HTS_GENERIC", "HTS_GENERIC_EXT", "CLIENT_DS_ID",
        "HGPID", "GRP_MPI", "LOCALGENERICDESC", "MSTRPROVID", "DISCONTINUEREASON", "ALLOWEDAMOUNT", "PAIDAMOUNT", "CHARGE", "DCC", "RXNORM_CODE", "ACTIVE_MED_FLAG", "ORIGIN")

    tables = List("temptable:crossix.parient.RxOrderBackEndMedMapping", "cdr.patient_mpi", "cdr.zh_provider_master_href")

    columnSelect = Map(
        "temptable:crossix.parient.RxOrderBackEndMedMapping" -> List(),
        "cdr.patient_mpi" -> List("PATIENTID","HGPID","GRP_MPI"),
        "cdr.zh_provider_master_href" -> List("MASTER_HGPROVID", "LOCALPROVIDERID")
    )


    beforeJoin = Map(
        "cdr.patient_mpi" -> ((df: DataFrame) => {df.distinct()}),
        "temptable:crossix.parient.RxOrderBackEndMedMapping" -> ((df: DataFrame) => {df.withColumnRenamed("GRP_MPI", "GRP_MPI1")})
    )

    join = (dfs: Map[String, DataFrame])  => {
        var patient_phi = dfs("cdr.patient_mpi")
        var rxOrderBackEndMedMapping = dfs("temptable:crossix.parient.RxOrderBackEndMedMapping")
        var zh_provider_master_href = dfs("cdr.zh_provider_master_href")

        rxOrderBackEndMedMapping.join(patient_phi, Seq("PATIENTID"), "left_outer")
                .join(zh_provider_master_href, Seq("LOCALPROVIDERID"), "left_outer").withColumnRenamed("MASTER_HGPROVID", "MSTRPROVID")

    }
}
