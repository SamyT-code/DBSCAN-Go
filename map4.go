/* * Samy Touabi - 300184721
 * CSI2520 Projet intégrateur - Partie 2 (concurrence avec Go)
 * Date: Hiver 2022
 *
 * Pour la version Go de votre projet, nous vous demandons d’exécuter, de façon concurrente, l’algorithme
 * DBSCAN sur des partitions des données de courses de taxis. Afin de créer ces partitions, vous devez
 * subdiviser le secteur géographique en une grille de NxN cellules.
 *
 * */

// Modification et complétion du ficher de Robert Laganière:
// Project CSI2120/CSI2520
// Winter 2022
// Robert Laganiere, uottawa.ca
// version 1.2

package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"strconv"
	"time"
)

type semaphore chan bool // Utiliser une sémaphore pour la synchronisation

func (s semaphore) Wait(n int) { // Méthode Wait des sémaphores
	for i := 0; i < n; i++ {
		<-s
	}
}

func (s semaphore) Signal() { // Méthode Signal des sémaphores
	s <- true
}

// Utiliser cette structure pour faire une channel de jobs afin
// d'implémenter le patron producteur/consommateur
type Job struct {
	id      int
	minsPts int
	eps     float64
	coords  []LabelledGPScoord
}

type GPScoord struct {
	lat  float64
	long float64
}

type LabelledGPScoord struct {
	GPScoord
	ID    int // point ID
	Label int // cluster ID
}

const Threads int = 1 // Cette constante est utilisée pour créer #Threads fils consommateurs
const N int = 4
const MinPts int = 5
const eps float64 = 0.0003
const filename string = "yellow_tripdata_2009-01-15_9h_21h_clean.csv"

func main() {

	start := time.Now()
	fmt.Println("N =", N, "and", Threads, "consumer threads \n ")

	gps, minPt, maxPt := readCSVFile(filename)
	fmt.Printf("Number of points: %d\n", len(gps))

	minPt = GPScoord{40.7, -74.}
	maxPt = GPScoord{40.8, -73.93}

	// geographical limits
	fmt.Printf("SW:(%f , %f)\n", minPt.lat, minPt.long)
	fmt.Printf("NE:(%f , %f) \n\n", maxPt.lat, maxPt.long)

	// Parallel DBSCAN STEP 1.
	incx := (maxPt.long - minPt.long) / float64(N)
	incy := (maxPt.lat - minPt.lat) / float64(N)

	var grid [N][N][]LabelledGPScoord // a grid of GPScoord slices

	// Create the partition
	// triple loop! not very efficient, but easier to understand

	partitionSize := 0
	for j := 0; j < N; j++ {
		for i := 0; i < N; i++ {

			for _, pt := range gps {

				// is it inside the expanded grid cell
				if (pt.long >= minPt.long+float64(i)*incx-eps) && (pt.long < minPt.long+float64(i+1)*incx+eps) && (pt.lat >= minPt.lat+float64(j)*incy-eps) && (pt.lat < minPt.lat+float64(j+1)*incy+eps) {

					grid[i][j] = append(grid[i][j], pt) // add the point to this slide
					partitionSize++
				}
			}
		}
	}

	// Parallel DBSCAN STEP 2.
	// Apply DBSCAN on each partition
	jobs := make(chan Job, N*N)   // jobs est un channel de Job
	mutex := make(semaphore, N*N) // mutex pour la synchronisation

	for i := 0; i < Threads; i++ { // Créer autant de go routines que de #Threads specifiés dans la constante plus haut
		go consomme(jobs, mutex)
	}

	// Insérer dans le channel jobs des nouveaux Jobs venant de grid[]][]
	for j := 0; j < N; j++ {
		for i := 0; i < N; i++ {
			jobs <- Job{i*10000000 + j*1000000, MinPts, eps, grid[i][j]}
		}
	}

	close(jobs) // Fermer le channel jobs

	mutex.Wait(Threads) // wait for consumers to terminate

	end := time.Now()
	fmt.Printf("\nExecution time: %s of %d points\n", end.Sub(start), partitionSize)
	fmt.Printf("Number of CPUs: %d", runtime.NumCPU())
}

// Fonction consomme modifiée à partir du fichier prodcons.go fourni
func consomme(jobs chan Job, sem semaphore) {
	for {
		j, more := <-jobs
		if more {
			DBscan(j.coords, j.minsPts, j.eps, j.id)
		} else {
			sem.Signal()
			return
		}
	}
}

// Applies DBSCAN algorithm on LabelledGPScoord points
// LabelledGPScoord: the slice of LabelledGPScoord points
// MinPts, eps: parameters for the DBSCAN algorithm
// offset: label of first cluster (also used to identify the cluster)
// returns number of clusters found
func DBscan(coords []LabelledGPScoord, MinPts int, eps float64, offset int) (nclusters int) {

	nclusters = 0 // Ce compteur servira, en partie, à identifier des Clusters

	for i := range coords { // Itérer à travers les coordonnées

		if coords[i].Label != 0 { // Si le point p n'est pas 0, alors il a déjà été visité.
			continue // On peut donc passer au prochain point
		}

		// Créer une slice neighbors qui contiendra tous les voisins de p selon les contraites
		neighbors := rangeQuery(coords, eps, coords[i])

		if len(neighbors) < MinPts {
			coords[i].Label = -1 // -1 Signifie que ce point est un "Noise"
			continue             // On peut donc passer au prochain point
		}

		nclusters++ // Icrémenter le nombre de Clusters

		coords[i].Label = nclusters // Donner à p le nouveau nombre de Clusters MAYBE DELETE THIS??

		var seedSet []*LabelledGPScoord // Voisins àa étendre
		seedSet = append(seedSet, neighbors...)

		for j := 0; j < len(seedSet); j++ {

			if seedSet[j].Label == -1 { // Si le point était un "Noise"", lui enlever ce statut de "Noise"
				seedSet[j].Label = nclusters
			}

			if (seedSet)[j].Label != 0 { // Si le point q n'est pas 0, alors il a déjà été visité.
				continue // On peut donc passer au prochain point
			}

			(seedSet)[j].Label = nclusters // Autrement, donner à q le label nClusters

			// Modifier la slice neighbors qui contiendra tous les voisins de q selon les contraites
			neighborsQ := rangeQuery(coords, eps, *seedSet[j])

			// Si neighborsQ countient suffisament de voisins, l'ajouter à seedSet
			if len(neighborsQ) >= MinPts {
				seedSet = append(seedSet, neighborsQ...)
			}

		}

	}

	// End of DBscan function
	// Printing the result (do not remove)
	fmt.Printf("Partition %10d : [%4d,%6d]\n", offset, nclusters, len(coords))

	return nclusters
}

// Applies RangeQuery helper function for DBSCAN algorithm
// LabelledGPScoord: the slice of LabelledGPScoord points
// eps: parameter for the RangeQuery algorithm (minimum distance between 2 points)
// q: LabelledGPScoord that is the "core" of the "neighborhood"
// returns pointer to  a slice of LabelledGPScoord
// EN RÉSUMÉ: Cette méthode détermine qui sont les points voisins d'un point et retourne l'ensemble des voisins
func rangeQuery(coords []LabelledGPScoord, eps float64, q LabelledGPScoord) []*LabelledGPScoord {

	var neighbors []*LabelledGPScoord // Créer une slice de TripRecord LabelledGPScoord

	for i := range coords { // Itérer à travers chaque point dans la base de données

		if coords[i].ID != q.ID && distance2(q.GPScoord, coords[i].GPScoord) <= eps { // Évaluer la distance et vérifier epsilon
			neighbors = append(neighbors, &coords[i]) // Ajouter coords[i] à neighbors
		}

	}
	return neighbors // neighbors // Retourner une liste de tous les voisins de q

}

// x1, y1, x2, y2: paramètres pour cet l'algorithme de distance
// Cette méthode trouve la distance euclidienne entre 2 points en 2D et la retourne. https://en.wikipedia.org/wiki/Euclidean_distance
func distance4(x1 float64, y1 float64, x2 float64, y2 float64) float64 {
	return math.Sqrt((y2-y1)*(y2-y1) + (x2-x1)*(x2-x1)) // Utilise
}

// x, y: paramètres pour cet l'algorithme de distance
// Cette méthode ce sert de la méthode distance4(...) pour trouver la distance entre 2 GPScoord et la retourne.
func distance2(x GPScoord, y GPScoord) float64 {
	return distance4(x.lat, x.long, y.lat, y.long)
}

// reads a csv file of trip records and returns a slice of the LabelledGPScoord of the pickup locations
// and the minimum and maximum GPS coordinates
func readCSVFile(filename string) (coords []LabelledGPScoord, minPt GPScoord, maxPt GPScoord) {

	coords = make([]LabelledGPScoord, 0, 5000)

	// open csv file
	src, err := os.Open(filename)
	defer src.Close()
	if err != nil {
		panic("File not found...")
	}

	// read and skip first line
	r := csv.NewReader(src)
	record, err := r.Read()
	if err != nil {
		panic("Empty file...")
	}

	minPt.long = 1000000.
	minPt.lat = 1000000.
	maxPt.long = -1000000.
	maxPt.lat = -1000000.

	var n int = 0

	for {
		// read line
		record, err = r.Read()

		// end of file?
		if err == io.EOF {
			break
		}

		if err != nil {
			panic("Invalid file format...")
		}

		// get lattitude
		lat, err := strconv.ParseFloat(record[9], 64)
		if err != nil {
			panic("Data format error (lat)...")
		}

		// is corner point?
		if lat > maxPt.lat {
			maxPt.lat = lat
		}
		if lat < minPt.lat {
			minPt.lat = lat
		}

		// get longitude
		long, err := strconv.ParseFloat(record[8], 64)
		if err != nil {
			panic("Data format error (long)...")
		}

		// is corner point?
		if long > maxPt.long {
			maxPt.long = long
		}

		if long < minPt.long {
			minPt.long = long
		}

		// add point to the slice
		n++
		pt := GPScoord{lat, long}
		coords = append(coords, LabelledGPScoord{pt, n, 0})
	}

	return coords, minPt, maxPt
}
