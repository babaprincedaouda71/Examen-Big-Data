package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class AvionAyantPlusIncidents {
    public static void main(String[] args) throws Exception {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Incidents en Continu")
                .master("local[*]")
                .getOrCreate();

        // Définir le schéma pour les fichiers CSV
        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType)
                .add("description", DataTypes.StringType)
                .add("no_avion", DataTypes.StringType)
                .add("date", DataTypes.StringType);

        // Lire les données CSV en continu depuis HDFS
        Dataset<Row> streamingDF = sparkSession.readStream()
                .option("header", "false")
                .schema(schema)
                .csv("hdfs://localhost:9000/exam-big-data");

        // Calculer le nombre d'incidents par avion
        Dataset<Row> incidentsCountDF = streamingDF.groupBy("no_avion").count();

        // Trier les avions par ordre décroissant du nombre d'incidents
        Dataset<Row> sortedDF = incidentsCountDF.orderBy(functions.desc("count"));

        // Afficher les résultats en continu
        StreamingQuery query = sortedDF.writeStream()
                .outputMode("complete")
                .format("console")
                .trigger(Trigger.ProcessingTime(10000))
                .start();

        query.awaitTermination();

    }
}
