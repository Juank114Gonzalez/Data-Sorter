import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.List;

public class Main {

    // Definir la cantidad de buckets como una constante global
    private static final int CANTIDAD_BUCKETS = 20;

    public static void main(String[] args) {
        String inputFile = "ejemplo.txt";
        String outputFile = "output.txt";

        long total = 0;

        long inicio = System.nanoTime();
        // Leer los datos del archivo
        List<String> data = readDataFromFile(inputFile);

        long fin = System.nanoTime();

        long tiempoTranscurrido = fin - inicio;

        // Convertir nanosegundos a milisegundos (1 segundo = 1,000,000,000 nanosegundos)
        long tiempoEnMilisegundos = tiempoTranscurrido / 1000000;

        // Imprimir el tiempo transcurrido
        System.out.println("Tiempo de ejecución: " + tiempoEnMilisegundos + " lectura");
        total += tiempoEnMilisegundos;

        inicio = System.nanoTime();

        for (int i = 0; i< 6; i++){
            data.addAll(data);
        }

        fin = System.nanoTime();

        tiempoTranscurrido = fin - inicio;

        tiempoEnMilisegundos = tiempoTranscurrido / 1000000;

        System.out.println("Tiempo de ejecución: " + tiempoEnMilisegundos + " duplicación");
        total += tiempoEnMilisegundos;

        System.out.println("Cantidad datos: " + data.size());

        // Dividir los datos en buckets
        List<List<String>> buckets = divideIntoBuckets(data);

        // Aplicar el Merge Sort de manera paralela a cada bucket
        inicio = System.nanoTime();
        data = parallelSortBuckets(buckets);
        fin = System.nanoTime();

       //mergeSort(data);



        //data.sort(String::compareTo);
        fin = System.nanoTime();

        tiempoTranscurrido = fin - inicio;
        tiempoEnMilisegundos = tiempoTranscurrido / 1000000;

        System.out.println("Tiempo de ejecución: " + tiempoEnMilisegundos + " ordenamiento");
        total += tiempoEnMilisegundos;


        inicio = System.nanoTime();
        // Escribir los datos ordenados en un nuevo archivo
        writeDataToFile(outputFile, data);

        fin = System.nanoTime();

        tiempoTranscurrido = fin - inicio;

        tiempoEnMilisegundos = tiempoTranscurrido / 1000000;

        System.out.println("Tiempo de ejecución: " + tiempoEnMilisegundos + " escritura");

        total += tiempoEnMilisegundos;

        System.out.println("Tiempo total de ejecución: " + total);
    }

    private static List<String> readDataFromFile(String fileName) {
        List<String> data = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = reader.readLine()) != null) {
                data.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }

    private static void writeDataToFile(String fileName, List<String> data) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            for (String line : data) {
                writer.write(line);
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static List<List<String>> divideIntoBuckets(List<String> data) {
        List<List<String>> buckets = new ArrayList<>();

        // Calcular la amplitud de cada bucket
        int min = 48;
        int max = 122;
        int amp = (max - min) / (int) (Math.sqrt(CANTIDAD_BUCKETS));

        // Inicializar buckets
        for (int i = 0; i < CANTIDAD_BUCKETS; i++) {
            buckets.add(new ArrayList<>());
        }

        // Distribuir elementos en buckets
        for (String value : data) {
            int bucketIndex = Math.min((value.charAt(0) - min) / amp, CANTIDAD_BUCKETS - 1);
            buckets.get(bucketIndex).add(value);
        }

        return buckets;
    }

    private static List<String> parallelSortBuckets(List<List<String>> buckets) {

        // Crear tareas para ordenar cada bucket de manera paralela
        List<RecursiveTask<List<String>>> tasks = new ArrayList<>();
        for (List<String> bucket : buckets) {
            tasks.add(new ParallelMergeSorter(bucket));
        }

        // Almacenar las tareas en forkJoinTasks
        List<RecursiveTask<List<String>>> forkJoinTasks = new ArrayList<>(tasks);

        // Iniciar la ejecución de cada tarea
        for (RecursiveTask<List<String>> task : forkJoinTasks) {
            task.fork();
        }

        // Recopilar los resultados ordenados de cada bucket
        List<String> sortedData = new ArrayList<>();
        for (RecursiveTask<List<String>> task : forkJoinTasks) {
            try {
                sortedData.addAll(task.join());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return sortedData;
    }
}

class ParallelMergeSorter extends RecursiveTask<List<String>> {
    private final List<String> data;

    public ParallelMergeSorter(List<String> data) {
        this.data = data;
    }

    @Override
    protected List<String> compute() {
        if (data.size() <= 1) {
            return data;
        }

        int middle = data.size() / 2;

        List<String> left = new ArrayList<>(data.subList(0, middle));
        List<String> right = new ArrayList<>(data.subList(middle, data.size()));

        ParallelMergeSorter leftSorter = new ParallelMergeSorter(left);
        ParallelMergeSorter rightSorter = new ParallelMergeSorter(right);

        invokeAll(leftSorter, rightSorter);

        return merge(leftSorter.join(), rightSorter.join());
    }

    private List<String> merge(List<String> left, List<String> right) {
        List<String> result = new ArrayList<>();
        int i = 0, j = 0;

        while (i < left.size() && j < right.size()) {
            if (left.get(i).compareTo(right.get(j)) < 0) {
                result.add(left.get(i++));
            } else {
                result.add(right.get(j++));
            }
        }

        while (i < left.size()) {
            result.add(left.get(i++));
        }

        while (j < right.size()) {
            result.add(right.get(j++));
        }

        return result;
    }

    public List<String> getResult() {
        return data;
    }
}