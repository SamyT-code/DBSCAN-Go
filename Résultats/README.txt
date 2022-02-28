Voici les spécifications de mon PC (4 coeurs):

Device name	LAPTOP-PBBFUNIM
Processor	Intel(R) Core(TM) i5-10210U CPU @ 1.60GHz   2.11 GHz
Installed RAM	8.00 GB (7.85 GB usable)
Device ID	FE5EF5A1-B490-455D-8DEA-B294E22E6D3C
Product ID	00325-81721-48365-AAOEM
System type	64-bit operating system, x64-based processor
Pen and touch	No pen or touch input is available for this display


Voici les résultats:

N=2 and 4 consumer threads - Execution time: 43.1340732s of 198194 points

N=4 and 4 consumer threads - Execution time: 18.1812498s of 206102 points

N=4 and 10 consumer threads - Execution time: 16.9928952s of 206102 points

N=10 and 4 consumer threads - Execution time: 4.8651584s of 222488 points

N=10 and 10 consumer threads - Execution time: 3.9974497s of 222488 points

N=10 and 50 consumer threads - Execution time: 4.1196359s of 222488 points

N=20 and 10 consumer threads - Execution time: 4.1016413s of 255300 points

N=20 and 50 consumer threads - Execution time: 3.0804774s of 255300 points

N=20 and 200 consumer threads - Execution time: 3.0990827s of 255300 points

BONUS: N=20 and 1000 consumer threads - Execution time: 2.9572129s of 255300 points

BONUS: N=20 and 10000 consumer threads - Execution time: 3.0352396s of 255300 points

BONUS: N=25 and 200 consumer threads - Execution time: 3.2323891s of 267856 points

BONUS: N=25 and 500 consumer threads - Execution time: 3.4564345s of 267856 points

BONUS: N=25 and 1000 consumer threads - Execution time: 3.2798873s of 267856 points

BONUS: N=100 and 500 consumer threads - EExecution time: 15.2887535s of 575776 points


Analyse des résultats:

	À première vue, il semble que le plus de fils d'exécutions qu'il y a, 
le plus rapidement le programme se termine. Également, il semble que le plus
élevé N soit, le plus rapidement le programme se terminera aussi.

	Cependant, on peut voir que si N et le nombre de fils sont très grands
(ex: N = 100 et 500 fils), le programme prend plus te temps à s'exécuter.
Parmis tous mes résultats, le plus rapide est quand N=20 avec 1000 fils (le programme
s'exécute juste en dessous de 3 secondes). Si on augmente trop N et/ou le nombre de fils,
le programme mettra plus de temps à s'exécuter (comme démontrer dans les BONUS).


