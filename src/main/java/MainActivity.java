import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class MainActivity {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRDD = sc.textFile("src/main/resources/input.txt");

        JavaRDD<String> initialtrimmedRDD = initialRDD.map(value -> value.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());

        JavaRDD<String> finalRDD = initialtrimmedRDD.filter(value -> value.trim().length() > 0);

        JavaRDD<String> words = finalRDD.flatMap(value -> Arrays.asList(value.split(" ")).iterator());

        JavaRDD<String> interestingwords = words.filter(Util::isNotBoring);

        JavaRDD<String> blanksremoved = interestingwords.filter(value -> value.trim().length()>0);

        JavaPairRDD<String, Long> pairRDD = blanksremoved.mapToPair(value -> new Tuple2<>(value, 1L));

        JavaPairRDD<String, Long> reduced = pairRDD.reduceByKey(Long::sum);

        JavaPairRDD<Long, String> reversed = reduced.mapToPair(value -> new Tuple2<>(value._2, value._1));

        List<Tuple2<Long, String>> list = reversed.sortByKey(false).take(20);

        list.forEach(System.out::println);

    }
}
