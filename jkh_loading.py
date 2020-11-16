from pyspark.sql.functions import *
from pyspark.sql.functions import lit
from pyspark.sql.types import *
import pyspark.sql.functions as F

snapDate = ""
snapdatekey = ""
file_extension = ".CSV"
delimiter = "|"

storageAccountName = dbutils.secrets.get(scope="slascope", key="SLAStgStorageAccountName")
blobAccessKey = dbutils.secrets.get(scope="slascope", key="SLAStgStorageAccountKey")
sqlDwUrlSmall = dbutils.secrets.get(scope="slascope", key="SLAStgDWHConnectionStringJDBCNoSSL")

blobStorage = "%(storageAccountName)s.blob.core.windows.net" % locals()
blobContainer = "jkhtest"
tempDir = "wasbs://" + blobContainer + "@" + blobStorage + "/temp"

spark.conf.set(
    "fs.azure.account.key.%(storageAccountName)s.blob.core.windows.net" % locals(),
    blobAccessKey)

# containerName = "bif-log-flight"

spark.conf.set(
    "fs.azure.account.key.%(storageAccountName)s.blob.core.windows.net" % locals(),
    blobAccessKey)

# connectionString = "wasbs://%s@%s.blob.core.windows.net/" % (containerName, storageAccountName)

def connectionString(containerName, storageAccountName, fileName):
    connectionString = "wasbs://%s@%s.blob.core.windows.net/new/%s" % (containerName, storageAccountName, fileName)
    return connectionString
containerName = "jkh"
fileName = "SAPItem" + file_extension

schema = StructType([
    StructField("SYSId", StringType()),
    StructField("RecordId", StringType()),
    StructField("Source", StringType()),
    StructField("BatchId", StringType()),
    StructField("SetId", StringType()),
    StructField("WhoAdd", StringType()),
    StructField("WhoEdit", StringType()),
    StructField("WhoDelete", StringType()),
    StructField("DateAdd", DateType()),
    StructField("DateToDelete", DateType()),
    StructField("DateToArchive", DateType()),
    StructField("VersionNumber", StringType()),
    StructField("IsLatest", StringType()),
    StructField("IsDeleted", StringType()),
    StructField("Level", StringType()),
    StructField("Code", StringType()),
    StructField("Description", StringType()),
    StructField("ITMUM", StringType()),
    StructField("Brand", StringType()),
    StructField("Unit", DoubleType ()),
    StructField("BrandCode", StringType()),
    StructField("ZEIAR", StringType()),
    StructField("CreateDate", DateType()),
    StructField("IsDCItem", StringType()),
    StructField("Department", StringType()),
    StructField("TaxClass", StringType()),
    StructField("SubDepartment", StringType()),
    StructField("Category", StringType()),
    StructField("SYSMCLevel", StringType())
])

DF_SAPItem = spark.read.schema(schema).option("delimiter", delimiter).csv(
    connectionString(containerName, storageAccountName, fileName))

containerName = "jkh"
fileName = "Location" + file_extension

schema = StructType([
    StructField("SYSId", IntegerType()),
    StructField("RecordId", StringType()),
    StructField("Source", StringType()),
    StructField("BatchId", IntegerType()),
    StructField("SetId", IntegerType()),
    StructField("WhoAdd", StringType()),
    StructField("WhoEdit", StringType()),
    StructField("WhoDelete", StringType()),
    StructField("DateAdd", DateType()),
    StructField("DateToDelete", DateType()),
    StructField("DateToArchive", DateType()),
    StructField("VersionNumber", StringType()),
    StructField("IsLatest", IntegerType()),
    StructField("IsDeleted", IntegerType()),
    StructField("LocationCode", StringType()),
    StructField("LocationName", StringType()),
    StructField("IsActive", IntegerType()),
    StructField("IsL4L", IntegerType()),
    StructField("TimeStampDW", DateType())
])

DF_Location = spark.read.schema(schema).option("delimiter", delimiter).csv(
    connectionString(containerName, storageAccountName, fileName))

containerName = "jkh"
fileName = "Supplier" + file_extension

schema = StructType([
    StructField("SYSId", IntegerType()),
    StructField("RecordId", StringType()),
    StructField("Source", StringType()),
    StructField("BatchId", IntegerType()),
    StructField("SetId", IntegerType()),
    StructField("WhoAdd", StringType()),
    StructField("WhoEdit", StringType()),
    StructField("WhoDelete", StringType()),
    StructField("DateAdd", DateType()),
    StructField("DateToDelete", DateType()),
    StructField("DateToArchive", DateType()),
    StructField("VersionNumber", StringType()),
    StructField("IsLatest", IntegerType()),
    StructField("IsDeleted", IntegerType()),
    StructField("VendorCode", StringType()),
    StructField("VendorName", StringType()),
    StructField("TimeStampDW", DateType())
])

DF_Supplier = spark.read.schema(schema).option("delimiter", delimiter).csv(
    connectionString(containerName, storageAccountName, fileName))

containerName = "jkh"
fileName = "InvoiceDetails" + file_extension

schema = StructType([
    StructField("SYSId", IntegerType()),
    StructField("RecordId", StringType()),
    StructField("Source", StringType()),
    StructField("BatchId", IntegerType()),
    StructField("SetId", IntegerType()),
    StructField("WhoAdd", StringType()),
    StructField("WhoEdit", StringType()),
    StructField("WhoDelete", StringType()),
    StructField("DateAdd", DateType()),
    StructField("DateToDelete", DateType()),
    StructField("DateToArchive", DateType()),
    StructField("VersionNumber", StringType()),
    StructField("IsLatest", IntegerType()),
    StructField("IsDeleted", IntegerType()),
    StructField("InvNo", StringType()),
    StructField("IssueDate", DateType()),
    StructField("LocationCode", StringType()),
    StructField("ItemCode", StringType()),
    StructField("Qty", IntegerType()),
    StructField("AvgCost", DoubleType()),
    StructField("IsVoid", IntegerType()),
    StructField("InvAmt", DoubleType()),
    StructField("PromoDisAmt", DoubleType()),
    StructField("LineDisAmt", DoubleType()),
    StructField("CompanyDrivenPromoAmt", DoubleType()),
    StructField("SplittedBillDisAmt", DoubleType()),
    StructField("TaxAmt", DoubleType()),
    StructField("ItemLineNo", IntegerType()),
    StructField("AccessUpdate", StringType()),
    StructField("IssueTime", DateType()),
    StructField("IsConfirmed", IntegerType()),
    StructField("CloseSales", StringType()),
    StructField("LineDisPer", StringType()),
    StructField("isCancel", IntegerType()),
    StructField("CashierId", StringType()),
    StructField("PromoCode", StringType()),
    StructField("PromoMode", StringType()),
    StructField("PromoType", StringType()),
    StructField("PromoDisVal", DoubleType()),
    StructField("SYSInvoiceId", IntegerType()),
    StructField("SYSItemId", IntegerType())
])

DF_InvoiceDetails = spark.read.schema(schema).option("delimiter", delimiter).csv(
    connectionString(containerName, storageAccountName, fileName))

containerName = "jkh"
fileName = "Invoice" + file_extension

schema = StructType([
    StructField("SYSId", IntegerType()),
    StructField("RecordId", StringType()),
    StructField("Source", StringType()),
    StructField("BatchId", IntegerType()),
    StructField("SetId", IntegerType()),
    StructField("WhoAdd", StringType()),
    StructField("WhoEdit", StringType()),
    StructField("WhoDelete", StringType()),
    StructField("DateAdd", DateType()),
    StructField("DateToDelete", DateType()),
    StructField("DateToArchive", DateType()),
    StructField("VersionNumber", StringType()),
    StructField("IsLatest", IntegerType()),
    StructField("IsDeleted", IntegerType()),
    StructField("InvNo", StringType()),
    StructField("IssueDate", DateType()),
    StructField("LocationCode", StringType()),
    StructField("TaxAmt", StringType()),
    StructField("CashierId", StringType()),
    StructField("Cash", DoubleType()),
    StructField("StationId", StringType()),
    StructField("IssueTime", DateType()),
    StructField("TmpCashierId", StringType()),
    StructField("ShiftNo", IntegerType()),
    StructField("StartTime", DateType()),
    StructField("NetAmt", IntegerType()),
    StructField("ItemWiseDis", IntegerType()),
    StructField("TotalDis", IntegerType()),
    StructField("PromoDis", IntegerType()),
    StructField("CashSaleAmt", DoubleType()),
    StructField("CreditCardAmt", DoubleType()),
    StructField("CouponAmt", DoubleType()),
    StructField("ChequeAmt", DoubleType()),
    StructField("FgnCurencySaleAmt", DoubleType()),
    StructField("GiftVoucherAmt", DoubleType()),
    StructField("MemberCode", StringType()),
    StructField("IsConfirmed", IntegerType()),
    StructField("OtherAmt", DoubleType()),
    StructField("CloseSales", StringType()),
    StructField("NexusMobile", StringType()),
    StructField("WebAmt", DoubleType()),
    StructField("SYSLocationId", IntegerType()),
    StructField("SYSOutletId", IntegerType())        
])

DF_Invoice = spark.read.schema(schema).option("delimiter", delimiter).csv(
    connectionString(containerName, storageAccountName, fileName))

containerName = "jkh"
fileName = "MCLevel" + file_extension

schema = StructType([
    StructField("SYSId", IntegerType()),
    StructField("RecordId", StringType()),
    StructField("Source", StringType()),
    StructField("BatchId", IntegerType()),
    StructField("SetId", IntegerType()),
    StructField("WhoAdd", StringType()),
    StructField("WhoEdit", StringType()),
    StructField("WhoDelete", StringType()),
    StructField("DateAdd", DateType()),
    StructField("DateToDelete", DateType()),
    StructField("DateToArchive", DateType()),
    StructField("VersionNumber", StringType()),
    StructField("IsLatest", IntegerType()),
    StructField("IsDeleted", IntegerType()),
    StructField("L4Code", StringType()),
    StructField("L2Code", StringType()),
    StructField("L1Code", StringType()),
    StructField("L0Code", StringType()),
    StructField("L4Desc", StringType()),
    StructField("L3Desc", StringType()),
    StructField("L2Desc", StringType()),
    StructField("L1Desc", StringType()),
    StructField("L0Desc", StringType()),
    StructField("L3Code", StringType()),
    StructField("TimeStampDW", DateType())
])

DF_MCLevel = spark.read.schema(schema).option("delimiter", delimiter).csv(
    connectionString(containerName, storageAccountName, fileName))

containerName = "jkh"
fileName = "OutletCluster" + file_extension

schema = StructType([
    StructField("SYSId", IntegerType()),
    StructField("RecordId", StringType()),
    StructField("Source", StringType()),
    StructField("BatchId", IntegerType()),
    StructField("SetId", IntegerType()),
    StructField("WhoAdd", StringType()),
    StructField("WhoEdit", StringType()),
    StructField("WhoDelete", StringType()),
    StructField("DateAdd", DateType()),
    StructField("DateToDelete", DateType()),
    StructField("DateToArchive", DateType()),
    StructField("VersionNumber", StringType()),
    StructField("IsLatest", IntegerType()),
    StructField("IsDeleted", IntegerType()),
    StructField("LocationCode", StringType()),
    StructField("SYSLocationId", IntegerType()),
    StructField("SubClusterName", StringType()),
    StructField("ClusterID", IntegerType()),
    StructField("ClusterName", StringType()),
    StructField("Profile", StringType()),
    StructField("SubClusterID", IntegerType()),
    StructField("TimeStampDW", DateType())
])

DF_OutletCluster = spark.read.schema(schema).option("delimiter", delimiter).csv(
    connectionString(containerName, storageAccountName, fileName))

containerName = "jkh"
fileName = "SAPItemLocation" + file_extension

schema = StructType([
    StructField("SYSId", IntegerType()),
    StructField("RecordId", StringType()),
    StructField("Source", StringType()),
    StructField("BatchId", IntegerType()),
    StructField("SetId", IntegerType()),
    StructField("WhoAdd", StringType()),
    StructField("WhoEdit", StringType()),
    StructField("WhoDelete", StringType()),
    StructField("DateAdd", DateType()),
    StructField("DateToDelete", DateType()),
    StructField("DateToArchive", DateType()),
    StructField("VersionNumber", StringType()),
    StructField("IsLatest", IntegerType()),
    StructField("IsDeleted", IntegerType()),
    StructField("ItemCode", StringType()),
    StructField("ItemLocation", StringType()),
    StructField("IMLLIFNR", StringType()),
    StructField("ItemAvgCost", DoubleType()),
    StructField("ItemStatus", StringType()),
    StructField("IsActive", IntegerType()),
    StructField("ItemCheckout", StringType()),
    StructField("ItemShelf", IntegerType()),
    StructField("ItemJanQty", IntegerType()),
    StructField("ItemFebQty", IntegerType()),
    StructField("ItemMarchQty", IntegerType()),
    StructField("ItemSepQty", IntegerType()),
    StructField("ItemOctQty", IntegerType()),
    StructField("ItemNovQty", IntegerType()),
    StructField("LastUpdatedDate", DateType()),
    StructField("SYSSAPItemSYSId", StringType()),
    StructField("SYSItemLocationId", IntegerType()),
    StructField("SYSSupplierId", IntegerType())
])

DF_SAPItemLocation = spark.read.schema(schema).option("delimiter", delimiter).csv(
    connectionString(containerName, storageAccountName, fileName))

DF_Invoice = DF_Invoice.drop("TaxAmt","IssueDate","LocationCode","InvNo") 
DF_OutletCluster = DF_OutletCluster.drop("LocationCode")
DF_Location = DF_Location.drop("LocationCode")
                       

tInvoice = DF_Invoice.alias('tInvoice')
tInvoiceDetails = DF_InvoiceDetails.alias('tInvoiceDetails')
tSAPItem = DF_SAPItem.alias('tSAPItem')
tMCLevel = DF_MCLevel.alias('tMCLevel')
tLocation = DF_Location.alias('tLocation')
tOutletCluster = DF_OutletCluster.alias('tOutletCluster')
tSAPItemLocation = DF_SAPItemLocation.alias('tSAPItemLocation')
tSupplier = DF_Supplier.alias('tSupplier')


DF_JKH = tInvoiceDetails.join(tInvoice, tInvoiceDetails.SYSInvoiceId == tInvoice.SYSId) \
                        .join(tSAPItem,tInvoiceDetails.SYSItemId == tSAPItem.SYSId) \
                        .join(tMCLevel, tSAPItem.SYSMCLevel == tMCLevel.SYSId) \
                        .join(tLocation, tInvoice.SYSLocationId == tLocation.SYSId) \
                        .join(tOutletCluster, tInvoice.SYSOutletId == tOutletCluster.SYSId) \
                        .join(tSAPItemLocation, tSAPItem.SYSId == tSAPItemLocation.SYSItemLocationId) \
                        .join(tSupplier, tSAPItemLocation.SYSSupplierId == tSupplier.SYSId).na.fill(0)

tDF_JKH = DF_JKH.alias('tDF_JKH')

DF_JKH = DF_JKH.drop("SYSId","RecordId","Source","SetId","WhoAdd","WhoEdit","WhoDelete","DateAdd","DateToDelete","Source","SetId","WhoAdd", \
                             "WhoEdit","WhoDelete","DateAdd","DateToDelete","DateToArchive","VersionNumber","IsLatest","IsDeleted","ItemCode", \
                             "ItemLineNo","AccessUpdate","IssueTime","IsConfirmed","CloseSales", "LineDisPer","isCancel","CashierId","PromoCode", \
                             "PromoType","PromoDisVal","SYSInvoiceId","Source","SetId","WhoAdd","WhoEdit","WhoDelete","DateAdd","DateToDelete", \
                             "DateToArchive","VersionNumber","IsLatest","IsDeleted","CashierId","Cash","StationId", \
                             "IssueTime","TmpCashierId","ShiftNo","StartTime","NetAmt", \
                             "ItemWiseDis","TotalDis","PromoDis","CashSaleAmt","CreditCardAmt","CouponAmt","ChequeAmt","FgnCurencySaleAmt", \
                             "GiftVoucherAmt","MemberCode","IsConfirmed","OtherAmt", \
                             "CloseSales","NexusMobile","WebAmt","SYSLocationId","SYSOutletId","Source","SetId","WhoAdd","WhoEdit","WhoDelete", \
                             "DateAdd","DateToDelete","DateToArchive","VersionNumber", \
                             "IsLatest","IsDeleted","ZEIAR","CreateDate","IsDCItem","Department","TaxClass","SubDepartment","Category", \
                             "SYSMCLevel","Source","SetId","WhoAdd","WhoEdit","WhoDelete", \
                             "DateAdd","DateToDelete","DateToArchive","VersionNumber","IsLatest","IsDeleted","L4Code","L2Code","L1Code", \
                             "L0CodeL3Code","TimeStampDW","Source","SetId","WhoAdd","WhoEdit", \
                             "WhoDelete","DateAdd","DateToDelete","DateToArchive","VersionNumber","IsLatest","IsDeleted","LocationName", \
                             "IsActive","IsL4L","TimeStampDW","Source","SetId","WhoAdd", \
                             "WhoEdit","WhoDelete","DateAdd","DateToDelete","DateToArchive","VersionNumber","IsLatest","IsDeleted", \
                             "SYSLocationId","SubClusterName","ClusterID","Profile","SubClusterID", \
                             "TimeStampDW","Source","SetId","WhoAdd","WhoEdit","WhoDelete","DateAdd","DateToDelete","DateToArchive", \
                             "VersionNumber","IsLatest","IsDeleted","ItemCode","ItemLocation" \
                             "IMLLIFNR","ItemAvgCost","ItemStatus","IsActive","ItemCheckout","ItemShelf","ItemJanQty","ItemFebQty", \
                             "ItemMarchQty","ItemSepQty","ItemOctQty","ItemNovQty", \
                             "LastUpdatedDate","SYSSAPItemSYSId","SYSItemLocationId","SYSSupplierId","Source","SetId","WhoAdd","WhoEdit", \
                             "WhoDelete","DateAdd","DateToDelete","DateToArchive", \
                             "VersionNumber","IsLatest","IsDeleted","TimeStampDW","BatchId")

DF_JKH = DF_JKH.withColumn("ClusterName", DF_OutletCluster['ClusterName'])

DF_JKH = DF_JKH.withColumn("Code", DF_SAPItem['Code']) \
                       .withColumn("Description", DF_SAPItem['Description']) \
                       .withColumn("Level", DF_SAPItem['Level']) 

DF_JKH = DF_JKH.withColumn("L4Desc", DF_MCLevel['L4Desc']) \
                       .withColumn("L3Desc", DF_MCLevel['L3Desc']) \
                       .withColumn("L2Desc", DF_MCLevel['L2Desc']) \
                       .withColumn("L1Desc", DF_MCLevel['L1Desc']) \
                       .withColumn("L0Desc", DF_MCLevel['L0Desc']) 

DF_JKH = DF_JKH.withColumn("BrandCode", DF_SAPItem['BrandCode']) \
                       .withColumn("Brand", DF_SAPItem['Brand']) 

DF_JKH = DF_JKH.withColumn("VendorCode", DF_Supplier['VendorCode']) \
                       .withColumn("VendorName", DF_Supplier['VendorName']) 

DF_JKH = DF_JKH.withColumn("LocationCode", DF_InvoiceDetails['LocationCode']) 

DF_JKH = DF_JKH.withColumn("ITMUM", DF_SAPItem['ITMUM']) 

DF_JKH = DF_JKH.withColumn("IssueDate", DF_InvoiceDetails['IssueDate']) \
                       .withColumn("SYSItemId", DF_JKH['SYSItemId']) 

DF_JKH_filtered = DF_JKH.where(DF_JKH['IsVoid'] == 0)

DF_JKH_filtered = DF_JKH_filtered.where(col("tInvoiceDetails.IssueDate").between('2019-12-01', '2019-12-14'))

DF_JKH_filtered=DF_JKH_filtered.withColumn("InvCnt", col("LocationCode") + col("InvNo"))
DF_JKH_filtered=DF_JKH_filtered.withColumn("Total_Cost", col("Qty")*col("AvgCost"))
DF_JKH_filtered=DF_JKH_filtered.withColumn("Tot_SaleVat", col("InvAmt") - col("TaxAmt")).na.fill(0)
DF_JKH_filtered=DF_JKH_filtered.withColumn("Tot_Weight", col("Qty")*col("Unit"))
DF_JKH_filtered=DF_JKH_filtered.withColumn("NetAmt", col("InvAmt") - col("PromoDisAmt") + col("CompanyDrivenPromoAmt")+ col("SplittedBillDisAmt")).na.fill(0)

# df=DF_InvoiceDetails.select(countDistinct((col("LocationCode") + col("InvNo"))).alias("InvCnt"))
# DF_JKH_filtered = DF_JKH_filtered.join(df)

DF_JKH_Final = DF_JKH_filtered.groupBy("ClusterName", "Code", "Description", "Level", \
                             "L4Desc", "L3Desc", "L2Desc","L1Desc", \
                             "L0Desc", "BrandCode", "Brand","VendorCode", \
                             "VendorName", "LocationCode", "ITMUM","IssueDate", \
                             "SYSItemId").agg(F.countDistinct("InvCnt").alias("InvCnt"),F.sum("Qty").alias("Qty"), F.sum("Total_Cost").alias("Total_Cost"), \
                                              F.sum("Tot_SaleVat").alias("Tot_SaleVat"), \
                                              F.sum("TaxAmt").alias("Tot_Vat"), F.sum("InvAmt").alias("Tot_Sale"), \
                                              F.sum("Tot_Weight").alias("Tot_Weight"), \
                                              F.sum("LineDisAmt").alias("LineDis"),F.sum("PromoDisAmt").alias("Promo_Amt"), \
                                              F.sum("CompanyDrivenPromoAmt").alias("Promo_Ou"), \
                                              F.sum("SplittedBillDisAmt").alias("SpitedBil"), F.sum("NetAmt").alias("NetAmt")).na.fill(0)
# DF_JKH_Final = DF_JKH_Final.join(df)
                                             
DF_JKH_Final.show()

