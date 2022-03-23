import sys
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession

def scenario1(spark):
    print("\nScenario 1:")
    print("Total number of consumers for Branch 1: ")
    spark.sql("select sum(counts.count) as total from branches join counts on branches.drink = counts.drink where branches.branch = 'Branch1'").show()

    print("\nTotal number of consumers for Branch 2: ")
    spark.sql("select sum(counts.count) as total from branches join counts on branches.drink = counts.drink where branches.branch = 'Branch2'").show()


def scenario2(spark):
    print("\nScenario 2:")
    print("Most consumed beverage of Branch 1:")
    spark.sql("select branches.drink, sum(counts.count) as total from branches join counts on branches.drink = counts.drink where branches.branch = 'Branch1' group by branches.drink order by sum(counts.count) desc limit 1").show()

    print("Most consumed beverage of Branch 2:")
    spark.sql("select branches.drink, sum(counts.count) as total from branches join counts on branches.drink = counts.drink where branches.branch = 'Branch2' group by branches.drink order by sum(counts.count) asc limit 1").show()

    print("Average of Branch 2 consumed beverages:")
    spark.sql("select round(avg(total), 0) from (Select branches.drink, sum(counts.count) as total from branches join counts on branches.drink = counts.drink where branches.branch = 'Branch2' group by branches.drink) as sumbranch2").show()


def scenario3(spark):
    print("\nScenario 3:")
    print("Beverages available on Branch10, Branch8, and Branch1:")
    spark.sql("select distinct drink from branches where branch = 'Branch10' or branch = 'Branch8' or branch = 'Branch1'").show()

    print("Common beverages available in Branch4 and Branch7:")
    spark.sql("select distinct drink from branches where branch = 'Branch4' intersect select distinct drink from branches where branch = 'Branch7'").show()


def scenario4(spark):
    #Partition
    print("\nScenario 4:")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    print("Partition of Scenario 3, part 1:")

    spark.sql("drop table if exists scenario4_t")

    print("creating table scenario4_t similar to branches and adding partition on branch...")
    spark.sql("create table scenario4_t(branch String, drink String) partitioned by (branch)")
    spark.sql("insert into scenario4_t select * from branches")

    print("Table statistics on table scenario4_t:")
    spark.sql("describe extended scenario4_t").show(50)

    print("Displaying Scenario 3 part 1 query on partitioned table...")
    spark.sql("select distinct drink from scenario4_t where branch = 'Branch10' or branch = 'Branch8' or branch = 'Branch1'").show()

    #View
    print("\nView of Scenario 3, part 2:")

    spark.sql("drop view if exists scenario4_v")

    print("Creating view of Scenario 3 part 2...")
    spark.sql("create view scenario4_v as (select distinct drink from branches where branch = 'Branch4' intersect select distinct drink from branches where branch = 'Branch7')")

    print("Table statistics on view:")
    spark.sql("describe extended scenario4_v").show(50)

    print("Displaying view...")
    spark.sql("select * from scenario4_v").show()


def scenario5(spark): 
    print("Inserting comment to scenario4 table...")
    spark.sql("comment on table scenario4_t is 'This is my comment on the table'")
    spark.sql("describe extended scenario4_t").show(50)

    print("Inserting note to scenario4 table...")
    spark.sql("alter table scenario4_t set tblproperties('notes' = 'This is my note')")
    spark.sql("show tblproperties scenario4_t").show()

    print("Deleting all entries of MED_Espresso drink from Scenario4_t")
    spark.sql("insert overwrite table scenario4_t select * from scenario4_t where drink != 'MED_Espresso'")
    spark.sql("select distinct drink from scenario4_t").show()


def scenario6(spark):
    # creating tables for each conscount file
    spark.sql("drop table if exists count2019")
    spark.sql("drop table if exists count2020")
    spark.sql("drop table if exists count2021")
    spark.sql("create table if not exists count2019(drink String,count Int) row format delimited fields terminated by ','")
    spark.sql("create table if not exists count2020(drink String,count Int) row format delimited fields terminated by ','")
    spark.sql("create table if not exists count2021(drink String,count Int) row format delimited fields terminated by ','")
    spark.sql("load data local inpath 'input/Bev_ConscountA.txt' into table count2019")
    spark.sql("load data local inpath 'input/Bev_ConscountB.txt' into table count2020")
    spark.sql("load data local inpath 'input/Bev_ConscountC.txt' into table count2021")

    print("Total sales for 2019, 2020, 2021:")
    spark.sql("select sum(count) as total_2019 from count2019").show()
    spark.sql("select sum(count) as total_2020 from count2020").show()
    spark.sql("select sum(count) as total_2021 from count2021").show()

    print("Sales growth from 2019-2020:")
    spark.sql("select (select sum(count) from count2020) - (select sum(count) from count2019) as 2020_2019_growth").show()

    print("Sales growth from 2020-2021:")
    spark.sql("select (select sum(count) from count2021) - (select sum(count) from count2020) as 2021_2020_growth").show()

    spark.sql("drop view if exists growth19_20")
    spark.sql("drop view if exists growth20_21")
    spark.sql("create view growth19_20 as (select (select sum(count) from count2020) - (select sum(count) from count2019) as diff)")
    spark.sql("create view growth20_21 as (select (select sum(count) from count2021) - (select sum(count) from count2020) as diff)")

    spark.sql("drop view if exists avg_growth")
    spark.sql("create view avg_growth as (select cast((growth19_20.diff + growth20_21.diff) as float)/2.0 as growth from growth19_20, growth20_21)")

    print("Estimated sales for 2022, using 2021 sales and average of growth:")
    spark.sql("select cast((total_21.total + avg_growth.growth) as int) as potential_total_2022 from (select sum(count) as total from count2021) as total_21, avg_growth").show()


if __name__ == "__main__":
    #create a spark session
    spark = SparkSession.builder.appName("PyProj1").config("spark.master", "local").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    #load input files into tables
    spark.sql("drop table if exists branches")
    spark.sql("drop table if exists counts")
    spark.sql("create table if not exists branches(drink String,branch String) row format delimited fields terminated by ','")
    spark.sql("create table if not exists counts(drink String,count Int) row format delimited fields terminated by ','")
    spark.sql("load data local inpath 'input/Bev_BranchA.txt' into table branches")
    spark.sql("load data local inpath 'input/Bev_BranchB.txt' into table branches")
    spark.sql("load data local inpath 'input/Bev_BranchC.txt' into table branches")

    spark.sql("load data local inpath 'input/Bev_ConscountA.txt' into table counts")
    spark.sql("load data local inpath 'input/Bev_ConscountB.txt' into table counts")
    spark.sql("load data local inpath 'input/Bev_ConscountC.txt' into table counts")
    
    #start menu
    print("---------------------------------------------")
    print("\nWelcome to Yash's Project 1")

    menu_option = 0
    while (menu_option != 7):
        print("\n- Select an option:\n" + "1. Scenario 1\n" + "2. Scenario 2\n" +
        "3. Scenario 3\n" + "4. Scenario 4\n" + "5. Scenario 5\n" + "6. Scenario 6\n" + "7. Exit")
        
        menu_option = int(input("\nOption: "))

        match menu_option:
            case 1: scenario1(spark)
            case 2: scenario2(spark)
            case 3: scenario3(spark)
            case 4: scenario4(spark)
            case 5: scenario5(spark)
            case 6: scenario6(spark)
            case 7: print("Goodbye!\n")
            case _: print("please enter valid input\n")

    #once exited, stopping spark and program
    spark.stop()
    sys.exit()
