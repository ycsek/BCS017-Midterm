package com.example.salesanalysis;

import java.io.*;
import java.util.*;

public class EuclideanRecommender {
    static class Product {
        String id;
        String category;
        double price;
        int quantity;
        int rating;
        public Product(String id, String category, double price, int quantity, int rating) {
            this.id = id;
            this.category = category;
            this.price = price;
            this.quantity = quantity;
            this.rating = rating;
        }
    }

    public static void main(String[] args) throws IOException {
        String inputPath = "sales_data.txt";
        String outputPath = "output/task3/recommend.txt";
        List<Product> products = new ArrayList<>();
        // 1. Get data from file
        try (BufferedReader br = new BufferedReader(new FileReader(inputPath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length != 5) continue;
                String id = parts[0];
                String category = parts[1];
                double price = Double.parseDouble(parts[2]);
                int quantity = Integer.parseInt(parts[3]);
                int rating = Integer.parseInt(parts[4]);
                products.add(new Product(id, category, price, quantity, rating));
            }
        }
        if (products.size() < 6) {
            System.out.println("You need at least 6 products to perform recommendation.");
            return;
        }
        // 2. Randomly select a product
        Random rand = new Random();
        int idx = rand.nextInt(products.size());
        Product target = products.get(idx);
        // 3. Calculate Euclidean distance and sort
        List<Product> others = new ArrayList<>();
        for (int i = 0; i < products.size(); i++) {
            if (i != idx) others.add(products.get(i));
        }
        others.sort(Comparator.comparingDouble(p -> euclidean(target, p)));
        // 4. Output top 5 recommendations
        try (PrintWriter pw = new PrintWriter(new FileWriter(outputPath))) {
            pw.println("Target product: " + target.id + ", " + target.category + ", " + target.price + ", " + target.quantity + ", " + target.rating);
            pw.println("Top-5 most similar products:");
            for (int i = 0; i < 5; i++) {
                Product rec = others.get(i);
                double dist = euclidean(target, rec);
                pw.printf("%d. %s, %s, %.2f, %d, %d, Dist: %.4f\n", i+1, rec.id, rec.category, rec.price, rec.quantity, rec.rating, dist);
            }
        }
        System.out.println("Results saved to: " + outputPath);
    }

    private static double euclidean(Product a, Product b) {
        double dp = a.price - b.price;
        double dq = a.quantity - b.quantity;
        double dr = a.rating - b.rating;
        return Math.sqrt(dp * dp + dq * dq + dr * dr);
    }
}
