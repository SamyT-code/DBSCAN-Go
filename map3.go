/* * Samy Touabi - 300184721
 * CSI2520 Projet intégrateur - Partie 1 (OOP avec Java)
 * Date: Hiver 2022
 *
 * On vous demande de programmer l’algorithme DBSCAN afin de grouper les différents enregistrements
 * en utilisant les coordonnées GPS des points de départ. Votre programme doit être une application Java
 * appelée TaxiClusters prenant en paramètre le nom du fichier contenant la base de données à
 * analyser, suivi des paramètres minPts et eps. Le programme produira en sortie la liste des groupes
 * dans un fichier csv donnant, pour chaque groupe, sa position (valeur moyenne des coordonnées de ses
 * points) et son nombre de points. Les points isolés sont ignorés.
 *
 * */

// Project CSI2120/CSI2520
// Winter 2022
// Robert Laganiere, uottawa.ca

package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"time"
)

type semaphore chan bool // Utiliser une sémaphore pour la synchronisation

func (s semaphore) Wait(n int) {
	for i := 0; i < n; i++ {
		<-s
	}
}

func (s semaphore) Signal() {
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

const Threads int = 1
const N int = 4
const MinPts int = 5
const eps float64 = 0.0003
const filename string = "yellow_tripdata_2009-01-15_9h_21h_clean.csv"

// func main() {

// 	start := time.Now()
// 	fmt.Println("N =", N, "and", Threads, "consumer threads \n ")

// 	gps, minPt, maxPt := readCSVFile(filename)
// 	fmt.Printf("Number of points: %d\n", len(gps))

// 	minPt = GPScoord{-74., 40.7}
// 	maxPt = GPScoord{-73.93, 40.8}

// 	// geographical limits
// 	fmt.Printf("NW:(%f , %f)\n", minPt.long, minPt.lat)
// 	fmt.Printf("SE:(%f , %f) \n\n", maxPt.long, maxPt.lat)

// 	// Parallel DBSCAN STEP 1.
// 	incx := (maxPt.long - minPt.long) / float64(N)
// 	incy := (maxPt.lat - minPt.lat) / float64(N)

// 	var grid [N][N][]LabelledGPScoord // a grid of GPScoord slices

// 	// Create the partition
// 	// triple loop! not very efficient, but easier to understand

// 	partitionSize := 0
// 	for j := 0; j < N; j++ {
// 		for i := 0; i < N; i++ {

// 			for _, pt := range gps {

// 				// is it inside the expanded grid cell
// 				if (pt.long >= minPt.long+float64(i)*incx-eps) && (pt.long < minPt.long+float64(i+1)*incx+eps) && (pt.lat >= minPt.lat+float64(j)*incy-eps) && (pt.lat < minPt.lat+float64(j+1)*incy+eps) {

// 					grid[i][j] = append(grid[i][j], pt) // add the point to this slide
// 					partitionSize++
// 				}
// 			}
// 		}
// 	}

// 	// Parallel DBSCAN STEP 2.
// 	// Apply DBSCAN on each partition
// 	jobs := make(chan Job, N*N)   // jobs est un channel de Job
// 	mutex := make(semaphore, N*N) // mutex pour la synchronisation

// 	for i := 0; i < Threads; i++ { // Créer autant de go routines que de threads specifiés dans la constante plus haut
// 		go consomme(jobs, mutex)
// 	}

// 	// Insérer dans le channel jobs des nouveaux Jobs venant de grid[]][]
// 	for j := 0; j < N; j++ {
// 		for i := 0; i < N; i++ {
// 			jobs <- Job{i*10000000 + j*1000000, MinPts, eps, grid[i][j]}
// 		}
// 	}

// 	close(jobs)

// 	mutex.Wait(Threads) // wait for consumers to terminate

// 	// Imprimer le temps d'exécution du programme et le nombre de points
// 	end := time.Now()
// 	fmt.Printf("\nExecution time: %s of %d points\n", end.Sub(start), partitionSize)

// }

func main() {

	start := time.Now()

	gps, minPt, maxPt := readCSVFile(filename)
	fmt.Printf("Number of points: %d\n", len(gps))

	minPt = GPScoord{-74., 40.7}
	maxPt = GPScoord{-73.93, 40.8}

	// geographical limits
	fmt.Printf("NW:(%f , %f)\n", minPt.long, minPt.lat)
	fmt.Printf("SE:(%f , %f) \n\n", maxPt.long, maxPt.lat)

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

	// ***
	// This is the non-concurrent procedural version
	// It should be replaced by a producer thread that produces jobs (partition to be clustered)
	// And by consumer threads that clusters partitions
	for j := 0; j < N; j++ {
		for i := 0; i < N; i++ {

			DBscan(grid[i][j], MinPts, eps, i*10000000+j*1000000)
			// fmt.Print(grid[0][3][0].Label)
		}
	}

	// Parallel DBSCAN STEP 2.
	// Apply DBSCAN on each partition
	// ...

	// Parallel DBSCAN step 3.
	// merge clusters
	// *DO NOT PROGRAM THIS STEP

	end := time.Now()
	fmt.Printf("\nExecution time: %s of %d points\n", end.Sub(start), partitionSize)
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

		var seedSet []*LabelledGPScoord
		seedSet = append(seedSet, neighbors...)
		// *seedSet = append(*seedSet, coords[i])
		// for j := 0; j < len(neighbors); j++{
		// 	*seedSet = append(*seedSet, *neighbors[j])
		// }

		// addNeighborstoSeedSet2(seedSet, neighbors) // ALTERNATE METHOD

		for j := 0; j < len(seedSet); j++ {

			if (seedSet)[j].Label == -1 {
				(seedSet)[j].Label = nclusters
				// fmt.Println("label change")
			}

			if (seedSet)[j].Label != 0 { // Si le point q n'est pas 0, alors il a déjà été visité.
				continue // On peut donc passer au prochain point
			}

			(seedSet)[j].Label = nclusters // Autrement, donner à q le label nClusters

			// Modifier la slice neighbors qui contiendra tous les voisins de q selon les contraites
			neighborsQ := rangeQuery(coords, eps, *seedSet[j])

			if len(neighborsQ) >= MinPts {
				// addNeighborstoSeedSet2(seedSet, neighborsQ) // ALTERNATE METHOD
				seedSet = append(seedSet, neighborsQ...)
				// seedSet = removeDuplicate(seedSet)

			}

		}

	}

	// End of DBscan function
	// Printing the result (do not remove)
	fmt.Printf("Partition %10d : [%4d,%6d]\n", offset, nclusters, len(coords))

	return nclusters
}

func removeDuplicate(slice *[]LabelledGPScoord) []LabelledGPScoord {
	allKeys := make(map[LabelledGPScoord]bool)
	list := []LabelledGPScoord{}
	for _, item := range *slice {
		if _, value := allKeys[item]; !value {
			allKeys[item] = true
			list = append(list, item)
		}
	}
	return list
}

// Cette fonction ajoute les voisins de p au seedSet mais a une valeur de retour
func addNeighborstoSeedSet2(seedSet *[]LabelledGPScoord, neighbors *[]LabelledGPScoord) {

	// var r []*LabelledGPScoord // Créer une slice de TripRecord LabelledGPScoord

	for i := 0; i < len(*neighbors); i++ { // Itérer à travers neighbours

		if !seedSetContainsP(*seedSet, (*neighbors)[i]) { // Si le seedSet ne contient pas p
			*seedSet = append(*seedSet, (*neighbors)[i]) // Alors, ajouter p au seedSet
		}

	}

	// return seedSet // Retourner seedset avec les voisins

}

// Cette fonction vérifie si une coordonnée p est déjà dans seedSet
func seedSetContainsP(seedSet []LabelledGPScoord, p LabelledGPScoord) bool {
	for i := range seedSet {
		if p == seedSet[i] {
			return true // Retourne true si p est dans seedSet
		}
	}

	return false // Sinon, retourne false
}

// Cette méthode détermine qui sont les points voisins d'un point
func rangeQuery(coords []LabelledGPScoord, eps float64, q LabelledGPScoord) []*LabelledGPScoord {

	var neighbors []*LabelledGPScoord // Créer une slice de TripRecord LabelledGPScoord

	for i := range coords {

		// if p.ID != q.ID && distance2(q.GPScoord, p.GPScoord) <= eps {
		if distance2(q.GPScoord, coords[i].GPScoord) <= eps {
			neighbors = append(neighbors, &coords[i])
		}

	}
	return neighbors // neighbors // Retourner une liste de tous les voisins de q

}

// Cette méthode trouve la distance euclidienne entre 2 points en 2D.
func distance4(x1 float64, y1 float64, x2 float64, y2 float64) float64 {
	return math.Sqrt((y2-y1)*(y2-y1) + (x2-x1)*(x2-x1))
}

// Cette méthode ce sert de la méthode distance4(...) pour trouver la distance entre 2 GPScoord.
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
		lat, err := strconv.ParseFloat(record[8], 64)
		if err != nil {
			fmt.Printf("\n%d lat=%s\n", n, record[8])
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
		long, err := strconv.ParseFloat(record[9], 64)
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
