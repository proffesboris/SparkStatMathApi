
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.log4j._
import org.apache.spark.sql.{SQLContext, SaveMode}
import java.util.Date

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions
import org.apache.spark.storage.StorageLevel



object ScalaSparkDev {

def main(args:Array[String]) {





  //Part for configuration
  Logger.getLogger("org").setLevel(Level.WARN)

  val conf = new SparkConf().setAppName("SmartSrcAggrReport")

  val NewSc = new SparkContext(conf)

  val hiveContext = new HiveContext(NewSc)

  val hiveContext1 = new HiveContext(NewSc)

  import hiveContext.implicits._


  var dateForName = new Date()

  val path_for_table = "/tmp/NewTestFold"+dateForName.getTime




  //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  //--------------------------------------------------------------------------------SmartSrc--------------------------------------------------------------------------------------------------------------------
  //-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



  val smartSrcHiveZ_client = broadcast(hiveContext1.sql("select c_inn, c_kpp, id as z_client_id from internal_eks_ibs.z_client").as("Z_client")).persist(StorageLevel.MEMORY_AND_DISK)
  val smartSrcHiveZ_main_docum = hiveContext1.sql("select c_num_dt,id, c_num_kt, c_nazn,c_sum, c_vid_doc, c_filial,c_kl_dt_2_3, c_kl_kt_2_3, c_kl_dt_2_2, c_kl_kt_2_2, c_kl_kt_1_1, c_kl_dt_2_inn, c_kl_dt_2_kpp, c_kl_kt_2_inn, c_kl_kt_2_kpp, c_date_prov from internal_eks_ibs.z_main_docum where c_date_prov > cast('2016-06-01 00:00:01.0' as timestamp) and c_date_prov < cast('2017-05-31 23:59:59.0' as timestamp)")
  val smartSrcHiveZ_records = hiveContext1.sql("select c_dt, c_date, collection_id, c_doc from internal_eks_ibs.z_records where c_date > cast('2016-06-01 00:00:01.0' as timestamp) and c_date < cast('2017-05-31 23:59:59.0' as timestamp)")
  val smartSrcHiveZ_ac_fin = broadcast(hiveContext1.sql("select c_fintool, c_client_v, c_arc_move from internal_eks_ibs.z_ac_fin").as("Z_ac_fin")).persist(StorageLevel.MEMORY_AND_DISK)



  smartSrcHiveZ_client
    .join(smartSrcHiveZ_ac_fin, smartSrcHiveZ_client("z_client_id") === smartSrcHiveZ_ac_fin("c_client_v"))
    .join(smartSrcHiveZ_records, smartSrcHiveZ_ac_fin("c_arc_move") === smartSrcHiveZ_records("collection_id"))
    .join(smartSrcHiveZ_main_docum, smartSrcHiveZ_records("c_doc") === smartSrcHiveZ_main_docum("id"))
    .write.mode(SaveMode.Overwrite).parquet(path_for_table)
  hiveContext1.createExternalTable("custom_cb_preapproval.smart_src_bor_partly_pre_final",path_for_table)


  Seq(smartSrcHiveZ_client,smartSrcHiveZ_ac_fin,smartSrcHiveZ_records,smartSrcHiveZ_main_docum).map(_.unpersist())



  val smartSrcsmart_src_bor_partly_pre_fin1 = hiveContext1.sql("select c_filial, c_vid_doc, c_fintool,  c_nazn document_desc, c_num_dt, c_num_kt, c_sum, c_inn, c_date_prov, c_dt, c_kl_kt_1_1 from custom_cb_preapproval.smart_src_bor_partly_pre_final")//.persist(StorageLevel.MEMORY_AND_DISK)

  ///  val smartSrcHiveZ_client2 = broadcast(hiveContext1.sql("select c_inn cl2_c_inn,c_kpp cl2_c_kpp,id id_c12 from internal_eks_ibs.z_client").as("Z_client2")).persist(StorageLevel.MEMORY_AND_DISK)
  val smartSrcHiveZ_branch = broadcast(hiveContext1.sql("select id id_br, c_local_code BRANCH_CD from internal_eks_ibs.z_branch").as("z_branch")).persist(StorageLevel.MEMORY_AND_DISK)
  val smartSrcHiveZ_name_paydoc = broadcast(hiveContext1.sql("select id id_pd,c_code from internal_eks_ibs.z_name_paydoc").as("z_name_paydoc")).persist(StorageLevel.MEMORY_AND_DISK)
  val smartSrcHiveZ_ft_money = broadcast(hiveContext1.sql("select c_code_iso, id id_ft from internal_eks_ibs.z_ft_money").as("z_ft_money")).persist(StorageLevel.MEMORY_AND_DISK)



  smartSrcsmart_src_bor_partly_pre_fin1.filter($"c_inn".isNotNull).join(smartSrcHiveZ_branch,smartSrcsmart_src_bor_partly_pre_fin1("c_fintool") === smartSrcHiveZ_branch("id_br"))
    .write.mode(SaveMode.Overwrite).parquet(path_for_table)
  hiveContext1.createExternalTable("custom_cb_preapproval.smart_src_bor_partly_pre_final_part1",path_for_table)

  Seq(smartSrcsmart_src_bor_partly_pre_fin1,smartSrcHiveZ_branch).map(_.unpersist())





  val smartSrcsmart_src_bor_partly_pre_fin2 = hiveContext1.sql("select * from custom_cb_preapproval.smart_src_bor_partly_pre_final_part1")//.persist(StorageLevel.MEMORY_AND_DISK)

  smartSrcsmart_src_bor_partly_pre_fin2.join(smartSrcHiveZ_name_paydoc,smartSrcsmart_src_bor_partly_pre_fin2("c_vid_doc") === smartSrcHiveZ_name_paydoc("id_pd"))//.persist(StorageLevel.MEMORY_AND_DISK)
    .write.mode(SaveMode.Overwrite).parquet(path_for_table)
  hiveContext1.createExternalTable("custom_cb_preapproval.smart_src_bor_partly_pre_final_part2",path_for_table)

  Seq(smartSrcsmart_src_bor_partly_pre_fin2,smartSrcHiveZ_name_paydoc).map(_.unpersist())



  val smartSrcsmart_src_bor_partly_pre_fin3 = hiveContext1.sql("select * from custom_cb_preapproval.smart_src_bor_partly_pre_final_part2")


  smartSrcsmart_src_bor_partly_pre_fin3.join(smartSrcHiveZ_ft_money,smartSrcsmart_src_bor_partly_pre_fin3("c_fintool") === smartSrcHiveZ_ft_money("id_ft"))
    .write.mode(SaveMode.Overwrite).parquet(path_for_table)
  hiveContext1.createExternalTable("custom_cb_preapproval.smart_src_bor_partly_pre_final4",path_for_table)


  Seq(smartSrcsmart_src_bor_partly_pre_fin3,smartSrcHiveZ_ft_money).map(_.unpersist())



 val smartSrcHiveTable2 = smartSrcHiveTable.select(smartSrcHiveTable("*"))
   .withColumn("DT",to_date(unix_timestamp($"pre_DT", "yyyy-MM-dd").cast("timestamp")))
   .withColumn("cutoff_to_date",to_date(unix_timestamp($"pre_cutoff", "yyyy-MM-dd").cast("timestamp")))//.cache()


val smartSrcHiveTable3 = smartSrcHiveTable2.select(
  smartSrcHiveTable2("inn"),
  smartSrcHiveTable2("DT"),
  smartSrcHiveTable2("cutoff_to_date"),
  smartSrcHiveTable2("to_d"),
  smartSrcHiveTable2("to_c"),
  smartSrcHiveTable2("sal0"),
  smartSrcHiveTable2("in1"),
  smartSrcHiveTable2("in2"),
  smartSrcHiveTable2("in4"),
  smartSrcHiveTable2("in5_inc"),
  smartSrcHiveTable2("sal"),
  smartSrcHiveTable2("sal_rec"),
  smartSrcHiveTable2("sal_nonc"),
  smartSrcHiveTable2("cash_in"),
  smartSrcHiveTable2("acq"),
  smartSrcHiveTable2("merch"),
  smartSrcHiveTable2("mbk"),
  smartSrcHiveTable2("inc_rent"),
  smartSrcHiveTable2("inc_trans"),
  smartSrcHiveTable2("selfcost"),
  smartSrcHiveTable2("transport"),
  smartSrcHiveTable2("rental"),
  smartSrcHiveTable2("ispoln"),
  smartSrcHiveTable2("pen_tax"),
  smartSrcHiveTable2("pen_oth"),
  smartSrcHiveTable2("tax_nds"),
  smartSrcHiveTable2("tax_profit"),
  smartSrcHiveTable2("tax_simp"),
  smartSrcHiveTable2("tax_unit"),
  smartSrcHiveTable2("tax_oth"),
  smartSrcHiveTable2("transout"),
  smartSrcHiveTable2("transin"),
  smartSrcHiveTable2("cashout"),
  smartSrcHiveTable2("lo_adv_cash"),
  smartSrcHiveTable2("onacc_cash"),
  smartSrcHiveTable2("onacc"),
  smartSrcHiveTable2("onacc_back"),
  smartSrcHiveTable2("lo_adv"),
  smartSrcHiveTable2("bus_out"),
  smartSrcHiveTable2("wage"),
  smartSrcHiveTable2("lo_receiv"),
  smartSrcHiveTable2("lo_repay"),
  smartSrcHiveTable2("lo_del_repay"),
  smartSrcHiveTable2("lo_early_repay")
)//.cache()




  val smartSrcHiveTable4 = smartSrcHiveTable3.select(smartSrcHiveTable3("*"))
      .withColumnRenamed("cutoff_to_date","cutoff")//.cache()



 //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 //--------------------------------------------------------------------------------Part for t2 table from sql script------------------------------------------------------------------------------------------
 //-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------







  //---------------------Windowing for first part----------------------------------------------------------------------------------------------------------------------------

  val WindowForfirstCumeDist = Window.partitionBy("inn","cutoff").orderBy("sal")
  val WindowForSecondCumeDist = Window.partitionBy("inn","cutoff").orderBy("sal_nonc")
  val WindowForAllRangeInFirstPart=Window.partitionBy("inn").orderBy(col("dt").cast("timestamp").cast("long")).rangeBetween(Long.MinValue,-1)

  //------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



  //---------------------Windowing for second part--------------------------------------------------------------------------------------------------------------------------------------------------

  val WindowForFirstCumeDistSecondPart = Window.partitionBy("inn","cutoff").orderBy("gap_sal")
  val WindowForSecondCumeDistSecondPart = Window.partitionBy("inn","cutoff").orderBy("gap_sal_nonc")
  val WindowForThirdCumeDistSecondPart = Window.partitionBy("inn","cutoff").orderBy("gap_sal_rec")

  //----------------------------------------------------------------------------------------------------------------------------------------------------------------------








  val smartSrcHiveTable5 = smartSrcHiveTable4
      .select(smartSrcHiveTable4("*"))
      .withColumn("sal_dist",round(cume_dist().over(WindowForfirstCumeDist),2))
      .withColumn("sal_nonc_dist", round(cume_dist().over(WindowForSecondCumeDist),2))
      .withColumn("gap_sal", smartSrcHiveTable4("dt").cast("timestamp").cast("long")-last(when(smartSrcHiveTable4("sal") > 0,smartSrcHiveTable4("dt"))).over(WindowForAllRangeInFirstPart).cast("timestamp").cast("long"))
      .withColumn("gap_sal_nonc",smartSrcHiveTable4("dt").cast("timestamp").cast("long")-last(when(smartSrcHiveTable4("sal_nonc") > 0, smartSrcHiveTable4("dt"))).over(WindowForAllRangeInFirstPart).cast("timestamp").cast("long"))
      .withColumn("gap_sal_rec",smartSrcHiveTable4("dt").cast("timestamp").cast("long")-last(when(smartSrcHiveTable4("sal_rec") > 0, smartSrcHiveTable4("dt"))).over(WindowForAllRangeInFirstPart).cast("timestamp").cast("long"))//.cache()





  //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  //--------------------------------------------------------------------------------Part for t3 table from sql script------------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------




  val smartSrcHiveTable6 = smartSrcHiveTable5.select(
    smartSrcHiveTable5("inn"),
    smartSrcHiveTable5("DT"),
    smartSrcHiveTable5("to_d"),
    smartSrcHiveTable5("to_c"),
    smartSrcHiveTable5("sal0"),
    smartSrcHiveTable5("in1"),
    smartSrcHiveTable5("in2"),
    smartSrcHiveTable5("in4"),
    smartSrcHiveTable5("in5_inc"),
    smartSrcHiveTable5("sal"),
    smartSrcHiveTable5("sal_rec"),
    smartSrcHiveTable5("sal_nonc"),
    smartSrcHiveTable5("cash_in"),
    smartSrcHiveTable5("acq"),
    smartSrcHiveTable5("merch"),
    smartSrcHiveTable5("mbk"),
    smartSrcHiveTable5("inc_rent"),
    smartSrcHiveTable5("inc_trans"),
    smartSrcHiveTable5("selfcost"),
    smartSrcHiveTable5("transport"),
    smartSrcHiveTable5("rental"),
    smartSrcHiveTable5("ispoln"),
    smartSrcHiveTable5("pen_tax"),
    smartSrcHiveTable5("pen_oth"),
    smartSrcHiveTable5("tax_nds"),
    smartSrcHiveTable5("tax_profit"),
    smartSrcHiveTable5("tax_simp"),
    smartSrcHiveTable5("tax_unit"),
    smartSrcHiveTable5("tax_oth"),
    smartSrcHiveTable5("transout"),
    smartSrcHiveTable5("transin"),
    smartSrcHiveTable5("cashout"),
    smartSrcHiveTable5("lo_adv_cash"),
    smartSrcHiveTable5("onacc_cash"),
    smartSrcHiveTable5("onacc"),
    smartSrcHiveTable5("onacc_back"),
    smartSrcHiveTable5("lo_adv"),
    smartSrcHiveTable5("bus_out"),
    smartSrcHiveTable5("wage"),
    smartSrcHiveTable5("lo_receiv"),
    smartSrcHiveTable5("lo_repay"),
    smartSrcHiveTable5("lo_del_repay"),
    smartSrcHiveTable5("lo_early_repay"),
    smartSrcHiveTable5("cutoff"),
    smartSrcHiveTable5("sal_dist"),
    smartSrcHiveTable5("sal_nonc_dist"),
    smartSrcHiveTable5("gap_sal"),
    smartSrcHiveTable5("gap_sal_nonc"),
    smartSrcHiveTable5("gap_sal_rec"),
    smartSrcHiveTable5("gap_sal"),
    smartSrcHiveTable5("gap_sal_nonc"),
    smartSrcHiveTable5("gap_sal_rec")
  ).withColumn("gap_sal_dist",round(cume_dist().over(WindowForFirstCumeDistSecondPart),2))
   .withColumn("gap_sal_nonc_dist",round(cume_dist().over(WindowForSecondCumeDistSecondPart),2))
   .withColumn("gap_sal_rec_dist",round(cume_dist().over(WindowForThirdCumeDistSecondPart),2))
   .withColumn("mth",month(smartSrcHiveTable5("cutoff")))//.cache()





  //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  //--------------------------------------------------------------------------------Part for t4 table from sql script------------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



  val smartSrcHiveTable7 = smartSrcHiveTable6.groupBy("inn", "cutoff","mth")
    .agg(
      sum($"sal").alias("sal"),
      round(sum(when(smartSrcHiveTable6("sal_dist") > 0.9, smartSrcHiveTable6("sal")).otherwise(0)) / sum($"sal"+0.1),2).alias("sal_90"),
      round(sum(when(smartSrcHiveTable6("sal_dist") > 0.95, smartSrcHiveTable6("sal")).otherwise(0)) / sum($"sal"+0.1),2).alias("sal_95"),
      min(when(smartSrcHiveTable6("gap_sal_dist") > 0.9, smartSrcHiveTable6("gap_sal")).otherwise(35)/60/60/24).alias("gap_sal_90"),
      min(when(smartSrcHiveTable6("gap_sal_nonc_dist") > 0.9, smartSrcHiveTable6("gap_sal")).otherwise(35)/60/60/24).alias("gap_sal_nonc_90"),
      min(when(smartSrcHiveTable6("gap_sal_rec_dist") > 0.9, smartSrcHiveTable6("gap_sal")).otherwise(35)/60/60/24).alias("gap_sal_rec_90"),
      sum($"cash_in").alias("cashin"),
      sum($"acq").alias("acq"),
      sum($"merch").alias("merch"),
      sum($"mbk").alias("mbk"),
      sum($"inc_rent").alias("inc_rent"),
      sum($"inc_trans").alias("inc_trans"),
      sum($"wage").alias("wage"),
      sum($"selfcost").alias("selfcost"),
      sum($"transport").alias("transport"),
      sum($"rental").alias("rental"),
      sum($"ispoln").alias("ispoln"),
      sum($"pen_tax").alias("pen_tax"),
      sum($"pen_oth").alias("pen_oth"),
      sum($"tax_nds").alias("tax_nds"),
      sum($"tax_oth").alias("tax_oth"),
      sum($"tax_profit").alias("tax_profit"),
      sum($"tax_simp").alias("tax_simp"),
      sum($"tax_unit").alias("tax_unit"),
      sum($"tax_oth"+$"tax_unit"+$"tax_simp"+$"tax_profit").alias("tax_sum"),
      sum($"tax_unit"+$"tax_simp"+$"tax_profit"+$"tax_profit").alias("tax_inc"),
      sum($"transin").alias("transin"),
      sum($"transout").alias("transout"),
      sum($"lo_receiv").alias("lo_receiv"),
      sum($"lo_repay").alias("lo_repay"),
      sum($"lo_del_repay").alias("lo_del_repay"),
      sum($"lo_early_repay").alias("lo_early_repay"),
      sum($"lo_adv").alias("lo_adv"),
      sum($"bus_out").alias("bus_out"),
      round(sum($"cashout") / sum($"sal"+0.1),2).alias("cashout_r"),
      round(sum($"lo_adv_cash") / sum($"sal"+0.1),2).alias("lo_adv_cash_r"),
      round(sum($"onacc_cash") / sum($"sal"+0.1),2).alias("onacc_cash_r"),
      round(sum($"onacc") / sum($"sal"+0.1),2).alias("onacc_r"),
      round(sum($"onacc_back") / sum($"sal"+0.1),2).alias("onacc_back_r"),
      sum($"to_d").alias("to_d"),
      sum($"to_c").alias("to_c")
    )//.cache()




  //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  //--------------------------------------------------------------------------------Part for t4 table from sql script------------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



//---------------------Windowing for pre-finish part--------------------------------------------------------------------------------------------------------------------------------------------------

  val WindowPartitionInn = Window.partitionBy("inn")
  val WindowPartitionInnOrderByCutoff = Window.partitionBy("inn").orderBy(desc("cutoff"))
  val WindowPartitionInnOrderByCutoffWithRange = Window.partitionBy("inn").orderBy(col("cutoff").cast("timestamp").cast("long").desc).rangeBetween(Long.MinValue,-1)




//----------------Windowing for cash block------------------------------------------------------------------------------------
  val WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_2 = Window.partitionBy("inn").orderBy(col("cutoff").cast("timestamp").cast("long").desc).rangeBetween(0,2)
  val WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_3 = Window.partitionBy("inn").orderBy(col("cutoff").cast("timestamp").cast("long").desc).rangeBetween(0,3)
  val WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5 = Window.partitionBy("inn").orderBy(col("cutoff").cast("timestamp").cast("long").desc).rangeBetween(0,5)
  val WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_6 = Window.partitionBy("inn").orderBy(col("cutoff").cast("timestamp").cast("long").desc).rangeBetween(0,6)
  val WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11 = Window.partitionBy("inn").orderBy(col("cutoff").cast("timestamp").cast("long").desc).rangeBetween(0,11)
  val WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_12 = Window.partitionBy("inn").orderBy(col("cutoff").cast("timestamp").cast("long").desc).rangeBetween(0,12)
  val WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_23 = Window.partitionBy("inn").orderBy(col("cutoff").cast("timestamp").cast("long").desc).rangeBetween(0,23)
  val WindowPartitionInnOrderByCutoffWithRangeInterval_3_5 = Window.partitionBy("inn").orderBy(col("cutoff").cast("timestamp").cast("long").desc).rangeBetween(3,5)
  val WindowPartitionInnOrderByCutoffWithRangeInterval_6_11 = Window.partitionBy("inn").orderBy(col("cutoff").cast("timestamp").cast("long").desc).rangeBetween(6,11)
  val WindowPartitionInnOrderByCutoffWithRangeInterval_12_23 = Window.partitionBy("inn").orderBy(col("cutoff").cast("timestamp").cast("long").desc).rangeBetween(12,23)


//----------------------------------------------------------------------------------------------------------------------------------------------------------------------




 val smartSrcHiveTable8 =  smartSrcHiveTable7.select("inn","cutoff","mth","sal","sal_90","sal_95","gap_sal_90","gap_sal_nonc_90","tax_profit","tax_simp","tax_unit","to_c","to_d","gap_sal_rec_90","cashin","acq","merch","mbk","inc_rent","inc_trans","wage","lo_del_repay","lo_repay","lo_receiv","ispoln","pen_tax","pen_oth","selfcost","transport","rental","bus_out","lo_adv","tax_nds","tax_oth","tax_sum")
    .withColumn("n",row_number().over(WindowPartitionInnOrderByCutoff))
    .withColumn("cnt",count("*").over(WindowPartitionInn))
    .withColumn("cutoff_max",max("cutoff").over(WindowPartitionInn))
    .withColumn("cutoff_min",min("cutoff").over(WindowPartitionInn))
    //------------------------------------Режим налообложения-------------------------------------------------------------------------------------------------------------
    .withColumn("tax_profit_fl",max(when(smartSrcHiveTable7("tax_profit") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRange))
    .withColumn("tax_simp_fl",max(when(smartSrcHiveTable7("tax_simp") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRange))
    .withColumn("tax_unit_fl",max(when(smartSrcHiveTable7("tax_unit") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRange))
   //------------------------------------------Выручка----------------------------------------------------------------------------------------------------------------------
    .withColumn("toc_avg_1_6",round(avg(when(smartSrcHiveTable7("mth").!==(1),smartSrcHiveTable7("to_c")).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5),2))
    .withColumn("delta_avg_1_6",round(avg(when(smartSrcHiveTable7("mth").!==(1),smartSrcHiveTable7("to_c")-smartSrcHiveTable7("to_d")).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5),2))
    .withColumn("sal_avg_1_6",round(avg(when(smartSrcHiveTable7("mth").!==(1),smartSrcHiveTable7("sal")).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5),2))
    .withColumn("sal_sum_1_6",round(sum(smartSrcHiveTable7("sal")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5),2))
    .withColumn("sal_stddev_1_6_pre",pow(when(smartSrcHiveTable7("mth").!==(1),smartSrcHiveTable7("sal")).otherwise(0)- avg(when (smartSrcHiveTable7("mth").!==(1),smartSrcHiveTable7("sal")).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5),2))
    .withColumn("toc_avg_1_12",round(avg(when(smartSrcHiveTable7("mth").!==(1),smartSrcHiveTable7("to_c")).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11),2))
    .withColumn("delta_avg_1_12",round(avg(when(smartSrcHiveTable7("mth").!==(1),smartSrcHiveTable7("to_c")-smartSrcHiveTable7("to_d")).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11),2))
    .withColumn("sal_avg_7_12",round(avg(when(smartSrcHiveTable7("mth").!==(1),smartSrcHiveTable7("sal")).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11),2))
    .withColumn("sal_sum_1_12",round(sum(smartSrcHiveTable7("sal")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11),2))
    .withColumn("sal_avg_1_12",round(avg(when(smartSrcHiveTable7("mth").!==(1),smartSrcHiveTable7("sal")).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11),2))
    .withColumn("sal_stddev_1_12_pre",pow(when(smartSrcHiveTable7("mth").!==(1),smartSrcHiveTable7("sal")).otherwise(0)- avg(when (smartSrcHiveTable7("mth").!==(1),smartSrcHiveTable7("sal")).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11),2))
    .withColumn("sal_90_1_6",round(avg(when(smartSrcHiveTable7("mth").!==(1),smartSrcHiveTable7("sal_90")).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5),2))
    .withColumn("sal_95_1_6",round(avg(when(smartSrcHiveTable7("mth").!==(1),smartSrcHiveTable7("sal_95")).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5),2))
    .withColumn("gap_sal_90_1_6",round(avg(when(smartSrcHiveTable7("mth").!==(1),smartSrcHiveTable7("gap_sal_90")).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5),2))
    .withColumn("gap_sal_nonc_90_1_6",round(avg(when(smartSrcHiveTable7("mth").!==(1),smartSrcHiveTable7("gap_sal_nonc_90")).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5),2))
    .withColumn("gap_sal_rec_90_1_6",round(avg(when(smartSrcHiveTable7("mth").!==(1),smartSrcHiveTable7("gap_sal_rec_90")).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5),2))
    .withColumn("cashin_r_1_6",round(sum(smartSrcHiveTable7("cashin")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5) / sum(smartSrcHiveTable7("sal")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5),2))
    .withColumn("acq_r_1_6",round(sum(smartSrcHiveTable7("acq")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5) / sum(smartSrcHiveTable7("sal")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5),2))
    .withColumn("merch_r_1_6",round(sum(smartSrcHiveTable7("merch")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5) / sum(smartSrcHiveTable7("sal")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5),2))
    .withColumn("mbk_r_1_6",round(sum(smartSrcHiveTable7("mbk")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5) / sum(smartSrcHiveTable7("sal")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5),2))
    .withColumn("inc_rent_r_1_6",round(sum(smartSrcHiveTable7("inc_rent")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5) / sum(smartSrcHiveTable7("sal")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5),2))
    .withColumn("inc_trans_r_1_6",round(sum(smartSrcHiveTable7("inc_trans")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5) / sum(smartSrcHiveTable7("sal")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5),2))
    .withColumn("sal_tr_13_46",round(avg(smartSrcHiveTable7("sal")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_2) / avg(smartSrcHiveTable7("sal")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeInterval_3_5),2))
    .withColumn("sal_tr_13_712",round(avg(smartSrcHiveTable7("sal")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_2) / avg(smartSrcHiveTable7("sal")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11),2))
    .withColumn("sal_tr_16_712",round(avg(smartSrcHiveTable7("sal")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5) / avg(smartSrcHiveTable7("sal")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11),2))
    .withColumn("sal_tr_16_1324",round(avg(smartSrcHiveTable7("sal")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5) / avg(smartSrcHiveTable7("sal")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23),2))
    .withColumn("sal_tr_112_1324",round(avg(smartSrcHiveTable7("sal")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11) / avg(smartSrcHiveTable7("sal")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23),2))
    .withColumn("sal_stddev_7_12_pre",pow(when(smartSrcHiveTable7("mth").!==(1),smartSrcHiveTable7("sal")).otherwise(0)- avg(when (smartSrcHiveTable7("mth").!==(1),smartSrcHiveTable7("sal")).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11),2))
   .withColumn("sal_stddev_13_24_pre",pow(when(smartSrcHiveTable7("mth").!==(1),smartSrcHiveTable7("sal")).otherwise(0)- avg(when (smartSrcHiveTable7("mth").!==(1),smartSrcHiveTable7("sal")).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23),2))
    // -------------------------------------------------------------- new for ip---------------------------------------------------------------------------
   .withColumn("sal_min_1_12",round(min(smartSrcHiveTable7("sal")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11)))
   //   ---------------------------------------------------------------- Зарплата----------------------------------------------------------------------
    .withColumn("wage_sum_1_6",sum(smartSrcHiveTable7("wage")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
     .withColumn("wage_avg_1_6",round(avg(smartSrcHiveTable7("wage")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5)))
     .withColumn("wage_cnt_1_6",sum(when(smartSrcHiveTable7("wage") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
     .withColumn("wage_stddev_1_6_pre",pow(smartSrcHiveTable7("wage")- avg(smartSrcHiveTable7("wage")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5),2))
     .withColumn("wage_stddev_1_12_pre",pow(smartSrcHiveTable7("wage")- avg(smartSrcHiveTable7("wage")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11),2))
     .withColumn("wage_tr_16_712",round(avg(smartSrcHiveTable7("wage")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5) / avg(smartSrcHiveTable7("wage")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11),2))
     .withColumn("wage_tr_16_1324",round(avg(smartSrcHiveTable7("wage")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5) / avg(smartSrcHiveTable7("wage")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23),2))
     .withColumn("wage_tr_112_1324",round(avg(smartSrcHiveTable7("wage")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11) / avg(smartSrcHiveTable7("wage")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23),2))
   // -------------------------------------------------------------- Кредитная история------------------------------------------------------------------------------------------------------------------------
      .withColumn("del_repay_cnt_1_6",sum(when(smartSrcHiveTable7("lo_del_repay") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
        .withColumn("del5_repay_cnt_1_6",sum(when(smartSrcHiveTable7("lo_del_repay") > 5,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
        .withColumn("del_repay_sum_1_6",sum(smartSrcHiveTable7("lo_del_repay")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
        .withColumn("del_repay_cnt_7_12",sum(when(smartSrcHiveTable7("lo_del_repay") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11))
        .withColumn("del5_repay_cnt_7_12",sum(when(smartSrcHiveTable7("lo_del_repay") > 5,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11))
       .withColumn("del_repay_sum_7_12",sum(smartSrcHiveTable7("lo_del_repay")).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11))
       .withColumn("del_repay_cnt_1_12",sum(when(smartSrcHiveTable7("lo_del_repay") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11))
        .withColumn("del5_repay_cnt_1_12",sum(when(smartSrcHiveTable7("lo_del_repay") > 5,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11))
        .withColumn("del_repay_sum_1_12",sum(smartSrcHiveTable7("lo_del_repay")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11))
        .withColumn("del_repay_cnt_13_24",sum(when(smartSrcHiveTable7("lo_del_repay") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23))
        .withColumn("del5_repay_cnt_13_24",sum(when(smartSrcHiveTable7("lo_del_repay") > 5,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23))
        .withColumn("del_repay_sum_13_24",sum(smartSrcHiveTable7("lo_del_repay")).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23))
      // ----------------------------------------------------------- Долговая нагрузка---------------------------------------------------------
       .withColumn("repay_sum_1_6",sum(smartSrcHiveTable7("lo_repay")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
        .withColumn("repay_avg_1_6",round(avg(smartSrcHiveTable7("lo_repay")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5)))
        .withColumn("repay_cnt_1_6",sum(when(smartSrcHiveTable7("lo_repay") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
        .withColumn("repay_stddev_1_6_pre",pow(smartSrcHiveTable7("lo_repay")- avg(smartSrcHiveTable7("lo_repay")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5),2))
        .withColumn("repay_sum_1_12",sum(smartSrcHiveTable7("lo_repay")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11))
        .withColumn("repay_avg_1_12",round(avg(smartSrcHiveTable7("lo_repay")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11)))
        .withColumn("repay_cnt_1_12",sum(when(smartSrcHiveTable7("lo_repay") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11))
       .withColumn("repay_stddev_1_12_pre",pow(smartSrcHiveTable7("lo_repay")- avg(smartSrcHiveTable7("lo_repay")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11),2))
     //---------------------------------------------------------------------- Полученные кредиты------------------------------------------------------------------------------------------------------
       .withColumn("receiv_sum_1_6",sum(smartSrcHiveTable7("lo_receiv")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
       .withColumn("receiv_avg_1_6",round(avg(smartSrcHiveTable7("lo_receiv")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5)))
       .withColumn("receiv_cnt_1_6",sum(when(smartSrcHiveTable7("lo_receiv") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
       .withColumn("receiv_sum_1_12",sum(smartSrcHiveTable7("lo_receiv")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11))
       .withColumn("receiv_avg_1_12",round(avg(smartSrcHiveTable7("lo_receiv")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11)))
       .withColumn("receiv_cnt_1_12",sum(when(smartSrcHiveTable7("lo_receiv") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11))
     // --------------------------------------------------------------- Полученные - погашенные кредиты---------------------------------------------
       .withColumn("lodelta_sum_1_6",sum(smartSrcHiveTable7("lo_repay")-smartSrcHiveTable7("lo_receiv")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
         .withColumn("lodelta_avg_1_6",round(avg(smartSrcHiveTable7("lo_repay")-smartSrcHiveTable7("lo_receiv")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5)))
         .withColumn("lodelta_sum_1_12",sum(smartSrcHiveTable7("lo_repay")-smartSrcHiveTable7("lo_receiv")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11))
         .withColumn("lodelta_avg_1_12",round(avg(smartSrcHiveTable7("lo_repay")-smartSrcHiveTable7("lo_receiv")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11)))
         .withColumn("repay_tr_13_46",round(avg(smartSrcHiveTable7("lo_repay")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_2) / avg(smartSrcHiveTable7("lo_repay")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeInterval_3_5),2))
         .withColumn("repay_tr_13_712",round(avg(smartSrcHiveTable7("lo_repay")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_2) / avg(smartSrcHiveTable7("lo_repay")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11),2))
         .withColumn("repay_tr_16_712",round(avg(smartSrcHiveTable7("lo_repay")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5) / avg(smartSrcHiveTable7("lo_repay")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11),2))
         .withColumn("repay_tr_16_1324",round(avg(smartSrcHiveTable7("lo_repay")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5) / avg(smartSrcHiveTable7("lo_repay")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23),2))
         .withColumn("repay_tr_112_1324",round(avg(smartSrcHiveTable7("lo_repay")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11) / avg(smartSrcHiveTable7("lo_repay")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23),2))
        //------------------------------------------------------------- Исполнительное-----------------------------------------------------------------------------------------------
    .withColumn("ispoln_cnt_1_6",sum(when(smartSrcHiveTable7("ispoln") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
        .withColumn("ispoln5_cnt_1_6",sum(when(smartSrcHiveTable7("ispoln") > 5,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
        .withColumn("ispoln_sum_1_6",sum(smartSrcHiveTable7("ispoln")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
        .withColumn("ispoln_cnt_7_12",sum(when(smartSrcHiveTable7("ispoln") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11))
        .withColumn("ispoln5_cnt_7_12",sum(when(smartSrcHiveTable7("ispoln") > 5,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11))
        .withColumn("ispoln_sum_7_12",sum(smartSrcHiveTable7("ispoln")).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11))
        .withColumn("ispoln_cnt_1_12",sum(when(smartSrcHiveTable7("ispoln") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11))
        .withColumn("ispoln5_cnt_1_12",sum(when(smartSrcHiveTable7("ispoln") > 5,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11))
        .withColumn("ispoln_sum_1_12",sum(smartSrcHiveTable7("ispoln")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11))
        .withColumn("ispoln_cnt_13_24",sum(when(smartSrcHiveTable7("ispoln") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23))
        .withColumn("ispoln5_cnt_13_24",sum(when(smartSrcHiveTable7("ispoln") > 5,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23))
        .withColumn("ispoln_sum_13_24",sum(smartSrcHiveTable7("ispoln")).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23))
        //---------------------------------------------------------------------------- Штрафы по налогам--------------------------------------------------------------------------
       .withColumn("pentax_cnt_1_6",sum(when(smartSrcHiveTable7("pen_tax") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
      .withColumn("pentax5_cnt_1_6",sum(when(smartSrcHiveTable7("pen_tax") > 5,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
      .withColumn("pentax_sum_1_6",sum(smartSrcHiveTable7("pen_tax")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
      .withColumn("pentax_cnt_7_12",sum(when(smartSrcHiveTable7("pen_tax") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11))
      .withColumn("pentax5_cnt_7_12",sum(when(smartSrcHiveTable7("pen_tax") > 5,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11))
      .withColumn("pentax_sum_7_12",sum(smartSrcHiveTable7("pen_tax")).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11))
      .withColumn("pentax_cnt_1_12",sum(when(smartSrcHiveTable7("pen_tax") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11))
      .withColumn("pentax5_cnt_1_12",sum(when(smartSrcHiveTable7("pen_tax") > 5,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11))
      .withColumn("pentax_sum_1_12",sum(smartSrcHiveTable7("pen_tax")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11))
      .withColumn("pentax_cnt_13_24",sum(when(smartSrcHiveTable7("pen_tax") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23))
      .withColumn("pentax5_cnt_13_24",sum(when(smartSrcHiveTable7("pen_tax") > 5,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23))
      .withColumn("pentax_sum_13_24",sum(smartSrcHiveTable7("pen_tax")).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23))
       //------------------------------------------------------------------------- Штрафы прочие--------------------------------------------------------------------------
      .withColumn("penoth_cnt_1_6",sum(when(smartSrcHiveTable7("pen_oth") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
     .withColumn("penoth5_cnt_1_6",sum(when(smartSrcHiveTable7("pen_oth") > 5,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
     .withColumn("penoth_sum_1_6",sum(smartSrcHiveTable7("pen_oth")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
     .withColumn("penoth_cnt_7_12",sum(when(smartSrcHiveTable7("pen_oth") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11))
     .withColumn("penoth5_cnt_7_12",sum(when(smartSrcHiveTable7("pen_oth") > 5,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11))
     .withColumn("penoth_sum_7_12",sum(smartSrcHiveTable7("pen_oth")).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11))
     .withColumn("penoth_cnt_1_12",sum(when(smartSrcHiveTable7("pen_oth") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11))
     .withColumn("penoth5_cnt_1_12",sum(when(smartSrcHiveTable7("pen_oth") > 5,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11))
     .withColumn("penoth_sum_1_12",sum(smartSrcHiveTable7("pen_oth")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11))
     .withColumn("penoth_cnt_13_24",sum(when(smartSrcHiveTable7("pen_oth") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23))
     .withColumn("penoth5_cnt_13_24",sum(when(smartSrcHiveTable7("pen_oth") > 5,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23))
     .withColumn("penoth_sum_13_24",sum(smartSrcHiveTable7("pen_oth")).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23))
    //-------------------------------------------------------------------------НДС-------------------------------------------------------------------------
     .withColumn("nds_cnt_1_6",sum(when(smartSrcHiveTable7("tax_nds") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
      .withColumn("nds_sum_1_6",sum(smartSrcHiveTable7("tax_nds")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
      .withColumn("nds_cnt_7_12",sum(when(smartSrcHiveTable7("tax_nds") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11))
      .withColumn("nds_sum_7_12",sum(smartSrcHiveTable7("tax_nds")).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11))
      .withColumn("nds_cnt_1_12",sum(when(smartSrcHiveTable7("tax_nds") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11))
      .withColumn("nds_sum_1_12",sum(smartSrcHiveTable7("tax_nds")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11))
      .withColumn("nds_cnt_13_24",sum(when(smartSrcHiveTable7("tax_nds") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23))
      .withColumn("nds_sum_13_24",sum(smartSrcHiveTable7("tax_nds")).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23))
     //-------------------------------------------------------------------------Налог на прибыль-------------------------------------------------------------------------
       .withColumn("taxprofit_cnt_1_6",sum(when(smartSrcHiveTable7("tax_profit") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
       .withColumn("taxprofit_avg_1_6",round(avg(smartSrcHiveTable7("tax_profit")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5)))
       .withColumn("taxprofit_cnt_7_12",sum(when(smartSrcHiveTable7("tax_profit") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11))
       .withColumn("taxprofit_avg_7_12",round(avg(smartSrcHiveTable7("tax_profit")).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11)))
       .withColumn("taxprofit_cnt_1_12",sum(when(smartSrcHiveTable7("tax_profit") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11))
       .withColumn("taxprofit_avg_1_12",round(avg(smartSrcHiveTable7("tax_profit")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11)))
       .withColumn("taxprofit_cnt_13_24",sum(when(smartSrcHiveTable7("tax_profit") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23))
       .withColumn("taxprofit_avg_13_24",round(avg(smartSrcHiveTable7("tax_profit")).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23)))
      //-------------------------------------------------------------------------Упрощенный налог-------------------------------------------------------------------------
       .withColumn("taxsimp_cnt_1_6",sum(when(smartSrcHiveTable7("tax_simp") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
       .withColumn("taxsimp_avg_1_6",round(avg(smartSrcHiveTable7("tax_simp")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5)))
       .withColumn("taxsimp_cnt_7_12",sum(when(smartSrcHiveTable7("tax_simp") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11))
       .withColumn("taxsimp_avg_7_12",round(avg(smartSrcHiveTable7("tax_simp")).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11)))
       .withColumn("taxsimp_cnt_1_12",sum(when(smartSrcHiveTable7("tax_simp") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11))
       .withColumn("taxsimp_avg_1_12",round(avg(smartSrcHiveTable7("tax_simp")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11)))
       .withColumn("taxsimp_cnt_13_24",sum(when(smartSrcHiveTable7("tax_simp") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23))
       .withColumn("taxsimp_avg_13_24",round(avg(smartSrcHiveTable7("tax_simp")).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23)))
      //-------------------------------------------------------------------------Единый налог-------------------------------------------------------------------------
   .withColumn("taxsimp_cnt_1_6",sum(when(smartSrcHiveTable7("tax_unit") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
      .withColumn("taxsimp_avg_1_6",round(avg(smartSrcHiveTable7("tax_unit")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5)))
      .withColumn("taxsimp_cnt_7_12",sum(when(smartSrcHiveTable7("tax_unit") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11))
      .withColumn("taxsimp_avg_7_12",round(avg(smartSrcHiveTable7("tax_unit")).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11)))
      .withColumn("taxsimp_cnt_1_12",sum(when(smartSrcHiveTable7("tax_unit") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11))
      .withColumn("taxsimp_avg_1_12",round(avg(smartSrcHiveTable7("tax_unit")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11)))
      .withColumn("taxsimp_cnt_13_24",sum(when(smartSrcHiveTable7("tax_unit") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23))
      .withColumn("taxsimp_avg_13_24",round(avg(smartSrcHiveTable7("tax_unit")).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23)))
      //-------------------------------------------------------------------------Налоги на доход-------------------------------------------------------------------------
  .withColumn("taxinc_cnt_1_6",sum(when(smartSrcHiveTable7("tax_unit") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
    .withColumn("taxinc_avg_1_6",round(avg(smartSrcHiveTable7("tax_unit")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5)))
    .withColumn("taxinc_cnt_7_12",sum(when(smartSrcHiveTable7("tax_unit") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11))
    .withColumn("taxinc_avg_7_12",round(avg(smartSrcHiveTable7("tax_unit")).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11)))
    .withColumn("taxinc_cnt_1_12",sum(when(smartSrcHiveTable7("tax_unit") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11))
    .withColumn("taxinc_avg_1_12",round(avg(smartSrcHiveTable7("tax_unit")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11)))
    .withColumn("taxinc_cnt_13_24",sum(when(smartSrcHiveTable7("tax_unit") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23))
    .withColumn("taxinc_avg_13_24",round(avg(smartSrcHiveTable7("tax_unit")).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23)))
    //-------------------------------------------------------------------------Страховые взносы-------------------------------------------------------------------------
     .withColumn("tax_cnt_1_6",sum(when(smartSrcHiveTable7("tax_oth") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
     .withColumn("tax_avg_1_6",round(avg(smartSrcHiveTable7("tax_oth")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5)))
     .withColumn("tax_cnt_7_12",sum(when(smartSrcHiveTable7("tax_oth") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11))
     .withColumn("tax_avg_7_12",round(avg(smartSrcHiveTable7("tax_oth")).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11)))
     .withColumn("tax_cnt_1_12",sum(when(smartSrcHiveTable7("tax_oth") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11))
     .withColumn("tax_avg_1_12",round(avg(smartSrcHiveTable7("tax_oth")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11)))
     .withColumn("tax_cnt_13_24",sum(when(smartSrcHiveTable7("tax_oth") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23))
     .withColumn("tax_avg_13_24",round(avg(smartSrcHiveTable7("tax_oth")).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23)))
    //-------------------------------------------------------------------------Суммарные налоги-------------------------------------------------------------------------
   .withColumn("taxsum_cnt_1_6",sum(when(smartSrcHiveTable7("tax_sum") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
    .withColumn("taxsum_avg_1_6",round(avg(smartSrcHiveTable7("tax_sum")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5)))
    .withColumn("taxsum_cnt_7_12",sum(when(smartSrcHiveTable7("tax_sum") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11))
    .withColumn("taxsum_avg_7_12",round(avg(smartSrcHiveTable7("tax_sum")).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11)))
    .withColumn("taxsum_cnt_1_12",sum(when(smartSrcHiveTable7("tax_sum") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11))
    .withColumn("taxsum_avg_1_12",round(avg(smartSrcHiveTable7("tax_sum")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11)))
    .withColumn("taxsum_cnt_13_24",sum(when(smartSrcHiveTable7("tax_sum") > 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23))
    .withColumn("taxsum_avg_13_24",round(avg(smartSrcHiveTable7("tax_sum")).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23)))
   //-------------------------------------------------------------------------Профиль расходов-------------------------------------------------------------------------
   .withColumn("selfcost_avg_1_6",round(avg(smartSrcHiveTable7("selfcost")+smartSrcHiveTable7("transport")+smartSrcHiveTable7("rental")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5)))
   .withColumn("transport_avg_1_6",round(avg(smartSrcHiveTable7("transport")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5)))
   .withColumn("rental_avg_1_6",round(avg(smartSrcHiveTable7("rental")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5)))
   .withColumn("profit_r_1_6",round(sum(smartSrcHiveTable7("sal")-smartSrcHiveTable7("selfcost")+smartSrcHiveTable7("transport")+smartSrcHiveTable7("rental")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5) / sum(smartSrcHiveTable7("sal")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5),2))
   .withColumn("selfcost_avg_1_12",round(avg(smartSrcHiveTable7("selfcost")+smartSrcHiveTable7("transport")+smartSrcHiveTable7("rental")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11)))
   .withColumn("transport_avg_1_12",round(avg(smartSrcHiveTable7("transport")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11)))
   .withColumn("rental_avg_1_12",round(avg(smartSrcHiveTable7("rental")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11)))
   .withColumn("profit_r_1_12",round(sum(smartSrcHiveTable7("sal")-smartSrcHiveTable7("selfcost")+smartSrcHiveTable7("transport")+smartSrcHiveTable7("rental")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11) / sum(smartSrcHiveTable7("sal")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11),2))
  //----------------------------------------------------------------Выведено из бизнеса--------------------------------------------------------------------------------------------------------
        .withColumn("busout_avg_1_6",round(avg(smartSrcHiveTable7("bus_out")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5)))
         .withColumn("busout_avg_7_12",round(avg(smartSrcHiveTable7("bus_out")).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11)))
         .withColumn("busout_avg_1_12",round(avg(smartSrcHiveTable7("bus_out")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11)))
         .withColumn("busout_avg_13_24",round(avg(smartSrcHiveTable7("bus_out")).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23)))
        //----------------------------------------------------------------Выдано кредитов--------------------------------------------------------------------------------------
        .withColumn("loadv_avg_1_6",round(avg(smartSrcHiveTable7("lo_adv")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5)))
         .withColumn("loadv_avg_7_12",round(avg(smartSrcHiveTable7("lo_adv")).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11)))
         .withColumn("loadv_avg_1_12",round(avg(smartSrcHiveTable7("lo_adv")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11)))
         .withColumn("loadv_avg_13_24",round(avg(smartSrcHiveTable7("lo_adv")).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23)))//.cache()


      val smartSrcHiveTable9 =  smartSrcHiveTable8.select(smartSrcHiveTable8("*"))
     .withColumn("sal_stddev_1_6",round(pow(sum("sal_stddev_1_6_pre").over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5)/(count("sal_stddev_1_6_pre").over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5)-1),0.5) /avg(when(smartSrcHiveTable8("mth").!==(1),smartSrcHiveTable8("sal")).otherwise(0)+0.1).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5),2))
     .withColumn("sal_stddev_1_12",round(pow(sum("sal_stddev_1_12_pre").over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11)/(count("sal_stddev_1_12_pre").over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11)-1),0.5) /avg(when(smartSrcHiveTable8("mth").!==(1),smartSrcHiveTable8("sal")).otherwise(0)+0.1).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11),2))
     .withColumn("sal_stddev_7_12",round(pow(sum("sal_stddev_7_12_pre").over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11)/(count("sal_stddev_7_12_pre").over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11)-1),0.5) /avg(when(smartSrcHiveTable8("mth").!==(1),smartSrcHiveTable8("sal")).otherwise(0)+0.1).over(WindowPartitionInnOrderByCutoffWithRangeInterval_6_11),2))
     .withColumn("sal_stddev_13_24",round(pow(sum("sal_stddev_13_24_pre").over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23)/(count("sal_stddev_13_24_pre").over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23)-1),0.5) /avg(when(smartSrcHiveTable8("mth").!==(1),smartSrcHiveTable8("sal")).otherwise(0)+0.1).over(WindowPartitionInnOrderByCutoffWithRangeInterval_12_23),2))
     .withColumn("wage_stddev_1_6",round(pow(sum("wage_stddev_1_6_pre").over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5)/(count("wage_stddev_1_6_pre").over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5)-1),0.5) /avg(smartSrcHiveTable8("wage")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5),2))
     .withColumn("wage_stddev_1_12",round(pow(sum("wage_stddev_1_12_pre").over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11)/(count("wage_stddev_1_12_pre").over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11)-1),0.5) /avg(smartSrcHiveTable8("wage")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11),2))
     .withColumn("repay_stddev_1_6",round(pow(sum("repay_stddev_1_6_pre").over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5)/(count("repay_stddev_1_6_pre").over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5)-1),0.5)/avg(smartSrcHiveTable8("lo_repay")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5),2))
     .withColumn("repay_stddev_1_12",round(pow(sum("repay_stddev_1_12_pre").over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11)/(count("repay_stddev_1_12_pre").over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11)-1),0.5)/avg(smartSrcHiveTable8("lo_repay")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_11),2))
     .withColumn("sal_6m_dev_max",round(max(smartSrcHiveTable8("sal")-smartSrcHiveTable8("sal_avg_1_6")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5) / sum(smartSrcHiveTable8("sal_avg_1_6")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5),2))
     .withColumn("sal_6m_dev_min",round(min(smartSrcHiveTable8("sal")-smartSrcHiveTable8("sal_avg_1_6")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5) / sum(smartSrcHiveTable8("sal_avg_1_6")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5),2))
     .withColumn("sal_6m_dev_neg_cnt",sum(when(smartSrcHiveTable8("sal") - smartSrcHiveTable8("sal_avg_1_6") < 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
     .withColumn("sal_6m_dev_max",round(max(smartSrcHiveTable8("sal")-smartSrcHiveTable8("sal_avg_1_12")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5) / sum(smartSrcHiveTable8("sal")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5),2))
     .withColumn("sal_6m_dev_min",round(min(smartSrcHiveTable8("sal")-smartSrcHiveTable8("sal_avg_1_12")).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5) / sum(smartSrcHiveTable8("sal")+0.1).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5),2))
     .withColumn("sal_6m_dev_neg_cnt",sum(when(smartSrcHiveTable8("sal") - smartSrcHiveTable8("sal_avg_1_12") < 0,1).otherwise(0)).over(WindowPartitionInnOrderByCutoffWithRangeIntervalCurrent_5))
     .withColumn("cutoff_s",smartSrcHiveTable8("cutoff").cast("string"))
     .withColumn("cutoff_max_s",smartSrcHiveTable8("cutoff_max").cast("string"))
     .withColumn("cutoff_min_s",smartSrcHiveTable8("cutoff_min").cast("string"))
    // .cache()

     //----------------------------------------------------------------------------------------------------------------------------------------------------------------------
     //------------------------------------------------------The final part for building table SmartSrcAggrReport----------------------------------------------------------------
     //----------------------------------------------------------------------------------------------------------------------------------------------------------------------

        val smartSrcHiveTable10 = smartSrcHiveTable9.select(
     smartSrcHiveTable9("inn"),
     smartSrcHiveTable9("sal_stddev_1_6"),
     smartSrcHiveTable9("sal_stddev_1_12"),
     smartSrcHiveTable9("sal_stddev_7_12"),
     smartSrcHiveTable9("sal_stddev_13_24"),
     smartSrcHiveTable9("wage_stddev_1_6"),
     smartSrcHiveTable9("wage_stddev_1_12"),
     smartSrcHiveTable9("repay_stddev_1_6"),
     smartSrcHiveTable9("repay_stddev_1_12"),
     smartSrcHiveTable9("cutoff_s"),
     smartSrcHiveTable9("mth"),
     smartSrcHiveTable9("sal"),
     smartSrcHiveTable9("sal_90"),
     smartSrcHiveTable9("sal_95"),
     smartSrcHiveTable9("gap_sal_90"),
     smartSrcHiveTable9("gap_sal_nonc_90"),
     smartSrcHiveTable9("tax_profit"),
     smartSrcHiveTable9("tax_simp"),
     smartSrcHiveTable9("tax_unit"),
     smartSrcHiveTable9("to_c"),
     smartSrcHiveTable9("to_d"),
     smartSrcHiveTable9("gap_sal_rec_90"),
     smartSrcHiveTable9("cashin"),
     smartSrcHiveTable9("acq"),
     smartSrcHiveTable9("merch"),
     smartSrcHiveTable9("mbk"),
     smartSrcHiveTable9("inc_rent"),
     smartSrcHiveTable9("inc_trans"),
     smartSrcHiveTable9("wage"),
     smartSrcHiveTable9("lo_del_repay"),
     smartSrcHiveTable9("lo_repay"),
     smartSrcHiveTable9("lo_receiv"),
     smartSrcHiveTable9("ispoln"),
     smartSrcHiveTable9("pen_tax"),
     smartSrcHiveTable9("pen_oth"),
     smartSrcHiveTable9("selfcost"),
     smartSrcHiveTable9("transport"),
     smartSrcHiveTable9("rental"),
     smartSrcHiveTable9("bus_out"),
     smartSrcHiveTable9("lo_adv"),
     smartSrcHiveTable9("tax_nds"),
     smartSrcHiveTable9("tax_oth"),
     smartSrcHiveTable9("tax_sum"),
     smartSrcHiveTable9("n"),
     smartSrcHiveTable9("cnt"),
     smartSrcHiveTable9("cutoff_max_s"),
     smartSrcHiveTable9("cutoff_min_s"),
     smartSrcHiveTable9("tax_profit_fl"),
     smartSrcHiveTable9("tax_simp_fl"),
     smartSrcHiveTable9("tax_unit_fl"),
     smartSrcHiveTable9("toc_avg_1_6"),
     smartSrcHiveTable9("delta_avg_1_6"),
     smartSrcHiveTable9("sal_avg_1_6"),
     smartSrcHiveTable9("sal_sum_1_6"),
     smartSrcHiveTable9("toc_avg_1_12"),
     smartSrcHiveTable9("delta_avg_1_12"),
     smartSrcHiveTable9("sal_avg_7_12"),
     smartSrcHiveTable9("sal_sum_1_12"),
     smartSrcHiveTable9("sal_avg_1_12"),
     smartSrcHiveTable9("sal_90_1_6"),
     smartSrcHiveTable9("sal_95_1_6"),
     smartSrcHiveTable9("gap_sal_90_1_6"),
     smartSrcHiveTable9("gap_sal_nonc_90_1_6"),
     smartSrcHiveTable9("gap_sal_rec_90_1_6"),
     smartSrcHiveTable9("cashin_r_1_6"),
     smartSrcHiveTable9("acq_r_1_6"),
     smartSrcHiveTable9("merch_r_1_6"),
     smartSrcHiveTable9("mbk_r_1_6"),
     smartSrcHiveTable9("inc_rent_r_1_6"),
     smartSrcHiveTable9("inc_trans_r_1_6"),
     smartSrcHiveTable9("sal_tr_13_46"),
     smartSrcHiveTable9("sal_tr_13_712"),
     smartSrcHiveTable9("sal_tr_16_712"),
     smartSrcHiveTable9("sal_tr_16_1324"),
     smartSrcHiveTable9("sal_tr_112_1324"),
     smartSrcHiveTable9("sal_min_1_12"),
     smartSrcHiveTable9("wage_sum_1_6"),
     smartSrcHiveTable9("wage_avg_1_6"),
     smartSrcHiveTable9("wage_cnt_1_6"),
     smartSrcHiveTable9("wage_tr_16_712"),
     smartSrcHiveTable9("wage_tr_16_1324"),
     smartSrcHiveTable9("wage_tr_112_1324"),
     smartSrcHiveTable9("del_repay_cnt_1_6"),
     smartSrcHiveTable9("del5_repay_cnt_1_6"),
     smartSrcHiveTable9("del_repay_sum_1_6"),
     smartSrcHiveTable9("del_repay_cnt_7_12"),
     smartSrcHiveTable9("del5_repay_cnt_7_12"),
     smartSrcHiveTable9("del_repay_sum_7_12"),
     smartSrcHiveTable9("del_repay_cnt_1_12"),
     smartSrcHiveTable9("del5_repay_cnt_1_12"),
     smartSrcHiveTable9("del_repay_sum_1_12"),
     smartSrcHiveTable9("del_repay_cnt_13_24"),
     smartSrcHiveTable9("del5_repay_cnt_13_24"),
     smartSrcHiveTable9("del_repay_sum_13_24"),
     smartSrcHiveTable9("repay_sum_1_6"),
     smartSrcHiveTable9("repay_avg_1_6"),
     smartSrcHiveTable9("repay_cnt_1_6"),
     smartSrcHiveTable9("repay_sum_1_12"),
     smartSrcHiveTable9("repay_avg_1_12"),
     smartSrcHiveTable9("repay_cnt_1_12"),
     smartSrcHiveTable9("receiv_sum_1_6"),
     smartSrcHiveTable9("receiv_avg_1_6"),
     smartSrcHiveTable9("receiv_cnt_1_6"),
     smartSrcHiveTable9("receiv_sum_1_12"),
     smartSrcHiveTable9("receiv_avg_1_12"),
     smartSrcHiveTable9("receiv_cnt_1_12"),
     smartSrcHiveTable9("lodelta_sum_1_6"),
     smartSrcHiveTable9("lodelta_avg_1_6"),
     smartSrcHiveTable9("lodelta_sum_1_12"),
     smartSrcHiveTable9("lodelta_avg_1_12"),
     smartSrcHiveTable9("repay_tr_13_46"),
     smartSrcHiveTable9("repay_tr_13_712"),
     smartSrcHiveTable9("repay_tr_16_712"),
     smartSrcHiveTable9("repay_tr_16_1324"),
     smartSrcHiveTable9("repay_tr_112_1324"),
     smartSrcHiveTable9("ispoln_cnt_1_6"),
     smartSrcHiveTable9("ispoln5_cnt_1_6"),
     smartSrcHiveTable9("ispoln_sum_1_6"),
     smartSrcHiveTable9("ispoln_cnt_7_12"),
     smartSrcHiveTable9("ispoln5_cnt_7_12"),
     smartSrcHiveTable9("ispoln_sum_7_12"),
     smartSrcHiveTable9("ispoln_cnt_1_12"),
     smartSrcHiveTable9("ispoln5_cnt_1_12"),
     smartSrcHiveTable9("ispoln_sum_1_12"),
     smartSrcHiveTable9("ispoln_cnt_13_24"),
     smartSrcHiveTable9("ispoln5_cnt_13_24"),
     smartSrcHiveTable9("ispoln_sum_13_24"),
     smartSrcHiveTable9("pentax_cnt_1_6"),
     smartSrcHiveTable9("pentax5_cnt_1_6"),
     smartSrcHiveTable9("pentax_sum_1_6"),
     smartSrcHiveTable9("pentax_cnt_7_12"),
     smartSrcHiveTable9("pentax5_cnt_7_12"),
     smartSrcHiveTable9("pentax_sum_7_12"),
     smartSrcHiveTable9("pentax_cnt_1_12"),
     smartSrcHiveTable9("pentax5_cnt_1_12"),
     smartSrcHiveTable9("pentax_sum_1_12"),
     smartSrcHiveTable9("pentax_cnt_13_24"),
     smartSrcHiveTable9("pentax5_cnt_13_24"),
     smartSrcHiveTable9("pentax_sum_13_24"),
     smartSrcHiveTable9("penoth_cnt_1_6"),
     smartSrcHiveTable9("penoth5_cnt_1_6"),
     smartSrcHiveTable9("penoth_sum_1_6"),
     smartSrcHiveTable9("penoth_cnt_7_12"),
     smartSrcHiveTable9("penoth5_cnt_7_12"),
     smartSrcHiveTable9("penoth_sum_7_12"),
     smartSrcHiveTable9("penoth_cnt_1_12"),
     smartSrcHiveTable9("penoth5_cnt_1_12"),
     smartSrcHiveTable9("penoth_sum_1_12"),
     smartSrcHiveTable9("penoth_cnt_13_24"),
     smartSrcHiveTable9("penoth5_cnt_13_24"),
     smartSrcHiveTable9("penoth_sum_13_24"),
     smartSrcHiveTable9("nds_cnt_1_6"),
     smartSrcHiveTable9("nds_sum_1_6"),
     smartSrcHiveTable9("nds_cnt_7_12"),
     smartSrcHiveTable9("nds_sum_7_12"),
     smartSrcHiveTable9("nds_cnt_1_12"),
     smartSrcHiveTable9("nds_sum_1_12"),
     smartSrcHiveTable9("nds_cnt_13_24"),
     smartSrcHiveTable9("nds_sum_13_24"),
     smartSrcHiveTable9("taxprofit_cnt_1_6"),
     smartSrcHiveTable9("taxprofit_avg_1_6"),
     smartSrcHiveTable9("taxprofit_cnt_7_12"),
     smartSrcHiveTable9("taxprofit_avg_7_12"),
     smartSrcHiveTable9("taxprofit_cnt_1_12"),
     smartSrcHiveTable9("taxprofit_avg_1_12"),
     smartSrcHiveTable9("taxprofit_cnt_13_24"),
     smartSrcHiveTable9("taxprofit_avg_13_24"),
     smartSrcHiveTable9("taxsimp_cnt_1_6"),
     smartSrcHiveTable9("taxsimp_avg_1_6"),
     smartSrcHiveTable9("taxsimp_cnt_7_12"),
     smartSrcHiveTable9("taxsimp_avg_7_12"),
     smartSrcHiveTable9("taxsimp_cnt_1_12"),
     smartSrcHiveTable9("taxsimp_avg_1_12"),
     smartSrcHiveTable9("taxsimp_cnt_13_24"),
     smartSrcHiveTable9("taxsimp_avg_13_24"),
     smartSrcHiveTable9("taxinc_cnt_1_6"),
     smartSrcHiveTable9("taxinc_avg_1_6"),
     smartSrcHiveTable9("taxinc_cnt_7_12"),
     smartSrcHiveTable9("taxinc_avg_7_12"),
     smartSrcHiveTable9("taxinc_cnt_1_12"),
     smartSrcHiveTable9("taxinc_avg_1_12"),
     smartSrcHiveTable9("taxinc_cnt_13_24"),
     smartSrcHiveTable9("taxinc_avg_13_24"),
     smartSrcHiveTable9("tax_cnt_1_6"),
     smartSrcHiveTable9("tax_avg_1_6"),
     smartSrcHiveTable9("tax_cnt_7_12"),
     smartSrcHiveTable9("tax_avg_7_12"),
     smartSrcHiveTable9("tax_cnt_1_12"),
     smartSrcHiveTable9("tax_avg_1_12"),
     smartSrcHiveTable9("tax_cnt_13_24"),
     smartSrcHiveTable9("tax_avg_13_24"),
     smartSrcHiveTable9("taxsum_cnt_1_6"),
     smartSrcHiveTable9("taxsum_avg_1_6"),
     smartSrcHiveTable9("taxsum_cnt_7_12"),
     smartSrcHiveTable9("taxsum_avg_7_12"),
     smartSrcHiveTable9("taxsum_cnt_1_12"),
     smartSrcHiveTable9("taxsum_avg_1_12"),
     smartSrcHiveTable9("taxsum_cnt_13_24"),
     smartSrcHiveTable9("taxsum_avg_13_24"),
     smartSrcHiveTable9("selfcost_avg_1_6"),
     smartSrcHiveTable9("transport_avg_1_6"),
     smartSrcHiveTable9("rental_avg_1_6"),
     smartSrcHiveTable9("profit_r_1_6"),
     smartSrcHiveTable9("selfcost_avg_1_12"),
     smartSrcHiveTable9("transport_avg_1_12"),
     smartSrcHiveTable9("rental_avg_1_12"),
     smartSrcHiveTable9("profit_r_1_12"),
     smartSrcHiveTable9("busout_avg_1_6"),
     smartSrcHiveTable9("busout_avg_7_12"),
     smartSrcHiveTable9("busout_avg_1_12"),
     smartSrcHiveTable9("busout_avg_13_24"),
     smartSrcHiveTable9("loadv_avg_1_6"),
     smartSrcHiveTable9("loadv_avg_7_12"),
     smartSrcHiveTable9("loadv_avg_1_12"),
     smartSrcHiveTable9("loadv_avg_13_24"))
  //.show()





                          smartSrcHiveTable10.write.mode(SaveMode.Overwrite).parquet(path_for_table)
                          hiveContext.createExternalTable("custom_cb_preapproval.SmartAggrReportFinal",path_for_table)



  //Seq(smartSrcHiveTable,smartSrcHiveTable2,smartSrcHiveTable3,smartSrcHiveTable4,smartSrcHiveTable5,smartSrcHiveTable6,smartSrcHiveTable7,smartSrcHiveTable8,smartSrcHiveTable9).map(_.unpersist())



  }



}
