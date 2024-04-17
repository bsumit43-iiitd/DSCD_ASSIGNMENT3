// Required modules
const fs = require('fs');

// Function to calculate Euclidean distance
function euclideanDistance(point1, point2) {
    let sum = 0;
    for (let i = 0; i < point1.length; i++) {
        sum += Math.pow(point1[i] - point2[i], 2);
    }
    return Math.sqrt(sum);
}

// Mapper function
function mapper(data, centroids) {
    let nearestCentroid;
    let minDistance = Number.MAX_VALUE;

    centroids.forEach((centroid, index) => {
        const distance = euclideanDistance(data, centroid);
        if (distance < minDistance) {
            minDistance = distance;
            nearestCentroid = index;
        }
    });

    return { key: nearestCentroid, value: data };
}

// Reducer function
function reducer(key, values) {
    const sum = values.reduce((acc, val) => {
        for (let i = 0; i < acc.length; i++) {
            acc[i] += val[i];
        }
        return acc;
    }, new Array(values[0].length).fill(0));

    const centroid = sum.map(val => val / values.length);
    return centroid;
}

// Function to initialize centroids
function initializeCentroids(k, data) {
    const centroids = [];
    for (let i = 0; i < k; i++) {
        const randomIndex = Math.floor(Math.random() * data.length);
        centroids.push(data[randomIndex]);
    }
    return centroids;
}

// Function to check convergence
function hasConverged(prevCentroids, newCentroids, epsilon) {
    for (let i = 0; i < prevCentroids.length; i++) {
        if (euclideanDistance(prevCentroids[i], newCentroids[i]) > epsilon) {
            return false;
        }
    }
    return true;
}

// Main function to run k-means
function kMeans(data, k, maxIterations, epsilon) {
    let centroids = initializeCentroids(k, data);
    let prevCentroids;

    for (let iter = 0; iter < maxIterations; iter++) {
        const clusters = new Array(k).fill().map(() => []);

        // Map step
        data.forEach(point => {
            const mapped = mapper(point, centroids);
            clusters[mapped.key].push(mapped.value);
        });

        prevCentroids = centroids;

        // Reduce step
        centroids = clusters.map(cluster => reducer(null, cluster));

        if (hasConverged(prevCentroids, centroids, epsilon)) {
            break;
        }
    }

    return centroids;
}

// Example usage
const data = [
    [1, 2],
    [3, 4],
    [5, 6],
    [7, 8],
    [9, 10],
    [11, 12]
];
const k = 4;
const maxIterations = 100;
const epsilon = 0.01;

const result = kMeans(data, k, maxIterations, epsilon);
console.log("Final centroids:", result);
