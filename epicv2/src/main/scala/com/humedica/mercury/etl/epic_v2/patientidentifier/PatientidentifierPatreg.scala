package com.humedica.mercury.etl.epic_v2.patientidentifier

import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.{EntitySource}
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 01/27/2017
 */


class PatientidentifierPatreg(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("temptable:epic_v2.patient.PatientTemptable", "cdr.map_predicate_values")

    beforeJoin = Map(
      "temptable" -> ((df: DataFrame) => {
        df.filter("ssn is not null").dropDuplicates(Seq("PATIENTID", "SSN"))
      })
    )



    map = Map(
      "DATASRC" -> literal("patreg"),
      "IDTYPE" -> literal("SSN"),
      "IDVALUE" -> mapFrom("SSN"),//, nullIf = Seq("000-00-0000", "999-99-9999", "888-88-8888", "111-11-1111", "333-33-3333", "222-22-2222", "555-55-5555", "777-77-7777", "444-44-4444", "123-45-6789")),
      "PATIENTID" -> mapFrom("PATIENTID")
    )

  }