package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class DeuxMoisAvecMoinsIncidents {
    public static void main(String[] args) throws Exception {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Deux ois avec le moins d'incidents")
                .master("local[*]")
                .getOrCreate();

        // Définir le schéma pour les fichiers CSV
        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType)
                .add("description", DataTypes.StringType)
                .add("no_avion", DataTypes.IntegerType)
                .add("date", DataTypes.StringType);

        // Lire les données CSV en continu depuis HDFS
        Dataset<Row> streamingDF = sparkSession.readStream()
                .option("header", "false")
                .schema(schema)
                .csv("hdfs://localhost:9000/exam-big-data");

        // Filtrer les incidents pour l'année en cours
        Dataset<Row> currentYearDF = streamingDF.filter(
                functions.year(functions.to_date(streamingDF.col("date"), "yyyy-MM-dd"))
                        .equalTo(functions.year(functions.current_date()))
        );

        // Calculer le nombre d'incidents par mois
        Dataset<Row> incidentsCountByMonthDF = currentYearDF.groupBy(
                functions.month(functions.to_date(streamingDF.col("date"), "yyyy-MM-dd")).alias("month")
        ).count();

        // Trier les mois par ordre croissant du nombre d'incidents
        Dataset<Row> sortedMonthsDF = incidentsCountByMonthDF.orderBy("month");

        // Afficher les résultats en continu
        StreamingQuery query = sortedMonthsDF.writeStream()
                .outputMode("complete")
                .format("console")
                .trigger(Trigger.ProcessingTime(10000))
                .start();

        query.awaitTermination();
    }
}
