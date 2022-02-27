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

	// "sync"
	"time"
)

type semaphore chan bool

func (s semaphore) Wait(n int) {
	for i := 0; i < n; i++ {
		<-s
	}
}

func (s semaphore) Signal() {
	s <- true
}

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

const Threads int = 4
const N int = 4
const MinPts int = 5
const eps float64 = 0.0003
const filename string = "yellow_tripdata_2009-01-15_9h_21h_clean.csv"

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
	// for j := 0; j < N; j++ {
	// 	for i := 0; i < N; i++ {

	// 		DBscan(grid[i][j], MinPts, eps, i*10000000+j*1000000)
	// 	}
	// }

	// Parallel DBSCAN STEP 2.
	// Apply DBSCAN on each partition
	// ...
	jobs := make(chan Job, 16) // SIZE MIGHT HAVE TO BE 15???
	// var wg sync.WaitGroup
	// wg.Add(N * N) // N * N devrait être 16
	mutex := make(semaphore, N*N)

	for i := 0; i < Threads; i++ {
		// go consomme(jobs, &wg)
	}

	for j := 0; j < N; j++ {
		for i := 0; i < N; i++ {
			jobs <- Job{i*10000000 + j*1000000, MinPts, eps, grid[i][j]}
		}
	}

	close(jobs)
	fmt.Println("jobs closed")
	// wg.Wait()
	mutex.Wait(Threads)
	fmt.Println("got to wg.Wait")

	// Parallel DBSCAN step 3.
	// merge clusters
	// *DO NOT PROGRAM THIS STEP

	end := time.Now()
	fmt.Printf("\nExecution time: %s of %d points\n", end.Sub(start), partitionSize)

}

// func consomme(jobs chan Job, done *sync.WaitGroup) {
func consomme(jobs chan Job, sem semaphore) {

	for {

		j, more := <-jobs

		if more {
			DBscan(j.coords, j.minsPts, j.eps, j.id)
		} else {
			// fmt.Println("Done")
			// done.Done()
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

	for _, p := range coords { // Itérer à travers les coordonnées

		if p.Label != 0 { // Si le point p n'est pas 0, alors il a déjà été visité.
			continue // On peut donc passer au prochain point
		}

		// Créer une slice neighbors qui contiendra tous les voisins de p selon les contraites
		neighbors := rangeQuery(coords, eps, p)

		if len(neighbors) < MinPts {
			p.Label = -1 // -1 Signifie que ce point est un "Noise"
			continue     // On peut donc passer au prochain point
		}

		nclusters++ // Icrémenter le nombre de Clusters

		p.Label = nclusters // Donner à p le nouveau nombre de Clusters

		var seedSet []LabelledGPScoord
		seedSet = append(seedSet, p)
		seedSet = addNeighborstoSeedSet2(seedSet, neighbors) // ALTERNATE METHOD

		for _, q := range seedSet {

			if q.Label == -1 {
				q.Label = nclusters
			}

			if q.Label != 0 { // Si le point q n'est pas 0, alors il a déjà été visité.
				continue // On peut donc passer au prochain point
			}

			q.Label = nclusters // Autrement, donner à q le label nClusters

			// Modifier la slice neighbors qui contiendra tous les voisins de q selon les contraites
			neighborsQ := rangeQuery(coords, eps, q)

			if len(neighborsQ) >= MinPts {
				seedSet = addNeighborstoSeedSet2(seedSet, neighborsQ) // ALTERNATE METHOD
			}

		}

	}

	// End of DBscan function
	// Printing the result (do not remove)
	fmt.Printf("Partition %10d : [%4d,%6d]\n", offset, nclusters, len(coords))

	return nclusters
}

// Cette fonction ajoute les voisins de p au seedSet mais a une valeur de retour
func addNeighborstoSeedSet2(seedSet []LabelledGPScoord, neighbors []LabelledGPScoord) []LabelledGPScoord {

	var r []LabelledGPScoord

	for _, p := range neighbors { // TRY WITH DIFFERENT KINDS OF PONBTERS

		if !seedSetContainsP(seedSet, p) {
			r = append(seedSet, p)
		}

	}

	return r

}

// Cette fonction vérifie si une coordonnée p est déjà dans seedSet
func seedSetContainsP(seedSet []LabelledGPScoord, p LabelledGPScoord) bool {
	for _, q := range seedSet {
		if p == q {
			return true // Retourne true si p est dans seedSet
		}
	}

	return false // Sinon, retourne false
}

func rangeQuery(coords []LabelledGPScoord, eps float64, q LabelledGPScoord) []LabelledGPScoord {

	var neighbors []LabelledGPScoord

	for _, p := range coords {

		if distance2(q.GPScoord, p.GPScoord) <= eps {
			neighbors = append(neighbors, p)
		}

	}
	return neighbors

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
