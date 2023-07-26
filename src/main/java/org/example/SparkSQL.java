package org.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SparkSQL {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Examen Big Data")
                .master("local[*]")
                .getOrCreate();

        // Lecture des données des tables
        Dataset<Row> volsDF = sparkSession.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/DB_AEROPORT")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", "VOLS")
                .option("user", "root")
                .option("password", "")
                .load();
        Dataset<Row> passagersDF = sparkSession.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/DB_AEROPORT")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", "PASSAGERS")
                .option("user", "root")
                .option("password", "")
                .load();
        Dataset<Row> reservationsDF = sparkSession.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/DB_AEROPORT")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", "RESERVATIONS")
                .option("user", "root")
                .option("password", "")
                .load();

        // Enregistrement des dataframes comme vues temporaires
        volsDF.createOrReplaceTempView("vols");
        passagersDF.createOrReplaceTempView("passagers");
        reservationsDF.createOrReplaceTempView("reservations");

        // Nombre de passagers par vol
        sparkSession.sql(
                "SELECT v.ID_VOL, v.DATE_DEPART, COUNT(r.ID_PASSAGER) AS NOMBRE " +
                        "FROM vols v " +
                        "LEFT JOIN reservations r ON v.ID_VOL = r.ID_VOL " +
                        "GROUP BY v.ID_VOL, v.DATE_DEPART").show();


        // Liste des vols en cours
        sparkSession.sql(
                "SELECT v.ID_VOL, v.DATE_DEPART, MAX(r.DATE_RESERVATION) AS DATE_ARRIVE " +
                        "FROM vols v " +
                        "JOIN reservations r ON v.ID_VOL = r.ID_VOL " +
                        "GROUP BY v.ID_VOL, v.DATE_DEPART").show();
    }
}