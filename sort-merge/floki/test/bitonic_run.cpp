
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <pthread.h>
#include <fstream>
#include <algorithm>
#include <math.h>
#include <iostream>
#include <omp.h>
#include <../floki/aa_sort.hpp>

using namespace std;
using namespace floki;

using namespace boost::simd;

using boost::tuples::tie;

struct timeval startwtime, endwtime, startstlsorttime, endstlsorttime, startqsorttime, endqsorttime;
double seq_time, qsort_time, stlsort_time;


int N,n,M;          
int *a;         
int *c;int *b;

char *table1, *table2, *displayOutput;

int threadlayers;

const int ASCENDING  = 1;
const int DESCENDING = 0;


void initA(void);
void initB(void);
void print(void);	
void test(void);
inline void exchange(int i, int j);
void compare(int i, int j, int dir);
inline void exchangeB(int i, int j);
void compareB(int i, int j, int dir);

void sort( void );
void impBitonicSort( void );
void recBitonicSort( int lo, int cnt, int dir );
void bitonicMerge( int lo, int cnt, int dir );
void bitonicMergeB( int lo, int cnt, int dir );

void Psort( void );
void * PrecBitonicSort( void * arg );
void * PbitonicMerge( void * arg );
void * PrecBitonicSortB( void * arg );
void * PbitonicMergeB( void * arg );
void PimpBitonicSort( void );

int desc( const void *a, const void *b ){
    int* arg1 = (int *)a;
    int* arg2 = (int *)b;
    if( *arg1 > *arg2 ) return -1;
    else if( *arg1 == *arg2 ) return 0;
    return 1;
}
int asc( const void *a, const void *b ){
    int* arg1 = (int *)a;
    int* arg2 = (int *)b;
    if( *arg1 < *arg2 ) return -1;
    else if( *arg1 == *arg2 ) return 0;
    return 1;
}

bool mycomparefunction (int * a,int * b) { return ( (*a) < (*b)); }


static __inline__ unsigned long long rdtsc(void)
{
  unsigned long long int x;
     __asm__ volatile (".byte 0x0f, 0x31" : "=A" (x));
     return x;
}


int main( int argc, char **argv ) {


    N = 1 << atoi( argv[ 1 ] );
    M = 1 << atoi( argv [ 2 ] );
    n = atoi( argv[ 3 ] );

    threadlayers = atoi( argv[ 3 ] );
    
    if( threadlayers != 0 && threadlayers != 1 ) {
	--threadlayers;
    }
    a = (int *) malloc( N * sizeof( int ) );
    b = (int *) malloc( M * sizeof( int ) );
    c = (int *)malloc((N) * sizeof(int));
    table1 = argv[4];
    table2 = argv[5];
    displayOutput = argv[6];
    
    initA();
    initB();

    initA();
    gettimeofday( &startstlsorttime, NULL );
    std::sort(a, a+N);
    gettimeofday( &endstlsorttime, NULL );

    initA();
    gettimeofday( &startqsorttime, NULL );
    qsort( a , N, sizeof( int ), asc );
    gettimeofday( &endqsorttime, NULL );
    
    initA();
    gettimeofday( &startwtime, NULL );
    Psort();
    gettimeofday( &endwtime, NULL );
    
    qsort_time = (double)( ( endwtime.tv_usec - startwtime.tv_usec ) / 1.0e6 + endwtime.tv_sec - startwtime.tv_sec );
    seq_time = (double)( ( endstlsorttime.tv_usec - startstlsorttime.tv_usec ) / 1.0e6 + endstlsorttime.tv_sec - startstlsorttime.tv_sec );
    stlsort_time = (double)( ( endqsorttime.tv_usec - startqsorttime.tv_usec ) / 1.0e6 + endqsorttime.tv_sec - startqsorttime.tv_sec );
    printf( "Bitonic parallel recursive and %i threads wall clock time = %f\n", 1 << atoi( argv[ 3 ] ), seq_time );
    printf( "Sequential C++ Sort and %i threads wall clock time = %f\n", 1 << atoi( argv[ 3 ] ), stlsort_time );
    //printf( "Sequential Q sort and %i threads wall clock time = %f\n", 1 << atoi( argv[ 3 ] ), qsort_time );
   // cout<<endl;
   // for(int i=0;i<N;i++) cout<<a[i]<<" ";
    //  cout<<endl;

    return 0;

}


void initA() {
  int i=0;
  ifstream f1(table1);
  string str;
  while(getline(f1, str)) {
    //cout<<str<<endl;
    string number = str.substr(0, str.find("|"));
    int val = stoi(number);
    a[i++] = val;
  } 
 // for (i = 0; i < N; i++) {
  //  a[i] = rand() % (N+1); // (N - i);
  //}
}

void initB() {
 int i=0;
  ifstream f1(table2);
  string str;
  while(getline(f1, str)) {
    //cout<<str<<endl;
    string number = str.substr(0, str.find("|"));
    int val = stoi(number);
    b[i++] = val;
  } 
 // for (i = 0; i < N; i++) {
  //  a[i] = rand() % (N+1); // (N - i);
  //}
}


void printA() {
  int i;
  for (i = 0; i < N; i++) {
    printf("%d\t", a[i]);
  }
  printf("\n");
}

void printB() {
  int i;
  for (i = 0; i < M; i++) {
    printf("%d\t", b[i]);
  }
  printf("\n");
}

inline void exchange(int i, int j) {
  int t;
  t = a[i];
  a[i] = a[j];
  a[j] = t;
}


inline void exchangeB(int i, int j) {
  int t;
  t = b[i];
  b[i] = b[j];
  b[j] = t;
}


inline void compare(int i, int j, int dir) {
  if (dir==(a[i]>a[j])) 
    exchange(i,j);
}


inline void compareB(int i, int j, int dir) {
  if (dir==(b[i]>b[j])) 
    exchangeB(i,j);
}


void bitonicMerge(int lo, int cnt, int dir) {
  if (cnt>1) {
    int k=cnt/2;
    int i;
    for (i=lo; i<lo+k; i++)
      compare(i, i+k, dir);
    bitonicMerge(lo, k, dir);
    bitonicMerge(lo+k, k, dir);
  }
}

void bitonicMergeB(int lo, int cnt, int dir) {
  if (cnt>1) {
    int k=cnt/2;
    int i;
    for (i=lo; i<lo+k; i++)
      compareB(i, i+k, dir);
    bitonicMergeB(lo, k, dir);
    bitonicMergeB(lo+k, k, dir);
  }
}



typedef struct{
    int lo, cnt, dir, layer;

} sarg;


void * PbitonicMerge( void * arg ){
    int lo = ( ( sarg * ) arg ) -> lo;
    int cnt = ( ( sarg * ) arg ) -> cnt;
    int dir = ( ( sarg * ) arg ) -> dir;
    int layer = ( ( sarg * ) arg ) -> layer;
    if( cnt > 1 ){
        int k = cnt / 2;
        int i;
        for( i = lo; i < lo + k; ++i ){
            compare(i, i + k, dir );
        }
        if( layer <= 0 ){
            bitonicMerge(lo, k, dir );
            bitonicMerge(lo + k, k, dir );
            return 0;
        }
        sarg arg1, arg2;
        pthread_t thread1, thread2;
        arg1.lo = lo;
        arg1.cnt = k;
        arg1.dir = dir;
        arg1.layer = layer - 1;
        arg2.lo = lo + k;
        arg2.cnt = k;
        arg2.dir = dir;
        arg2.layer = layer - 1;
        pthread_create( &thread1, NULL, PbitonicMerge, &arg1 );
        pthread_create( &thread2, NULL, PbitonicMerge, &arg2 );
        
        pthread_join( thread1, NULL );
        pthread_join( thread2, NULL );
    }
    return 0;
}

void * PbitonicMergeB( void * arg ){
    int lo = ( ( sarg * ) arg ) -> lo;
    int cnt = ( ( sarg * ) arg ) -> cnt;
    int dir = ( ( sarg * ) arg ) -> dir;
    int layer = ( ( sarg * ) arg ) -> layer;
    if( cnt > 1 ){
        int k = cnt / 2;
        int i;
        for( i = lo; i < lo + k; ++i ){
            compareB(i, i + k, dir );
        }
        if( layer <= 0 ){
            bitonicMergeB(lo, k, dir );
            bitonicMergeB(lo + k, k, dir );
            return 0;
        }
        sarg arg1, arg2;
        pthread_t thread1, thread2;
        arg1.lo = lo;
        arg1.cnt = k;
        arg1.dir = dir;
        arg1.layer = layer - 1;
        arg2.lo = lo + k;
        arg2.cnt = k;
        arg2.dir = dir;
        arg2.layer = layer - 1;
        pthread_create( &thread1, NULL, PbitonicMergeB, &arg1 );
        pthread_create( &thread2, NULL, PbitonicMergeB, &arg2 );
        
        pthread_join( thread1, NULL );
        pthread_join( thread2, NULL );
    }
    return 0;
}


template <typename pack_t>
static bool are_packs_equal(const pack_t &lhs, const pack_t &rhs)
{
    for (int i = 0; i < pack_t::static_size; ++i)
        if (lhs[i] != rhs[i])
            return false;

    return true;
}

void * PrecBitonicSort( void * arg ){
    int lo = ( ( sarg * ) arg ) -> lo;
    int cnt = ( ( sarg * ) arg ) -> cnt;
    int dir = ( ( sarg * ) arg ) -> dir;
    int layer = ( ( sarg * ) arg ) -> layer;

    if ( cnt > 1 ) {
        int k = cnt / 2;
        if( layer >= threadlayers ) {
            qsort( a + lo, k, sizeof( int ), asc );
            qsort( a + ( lo + k ) , k, sizeof( int ), desc );
        }
       // cout<<"K value: "<<k<<endl;
       /* if(cnt==32) {
          // cout<<"inside again."<<endl;
           using pack_t = pack<int32_t, 4>;
            std::vector<pack_t> values{ { a[lo+0], a[lo+1], a[lo+2], a[lo+3] },
                                        { a[lo+4], a[lo+5], a[lo+6], a[lo+7] },
                                        { a[lo+8], a[lo+9], a[lo+10], a[lo+11] },
                                        { a[lo+12], a[lo+13], a[lo+14], a[lo+15] } };
           //sort using bitonic sort
            tie(values[0], values[1], values[2], values[3]) = floki::detail::bitonic_sort_16(values[0], values[1],
                                                 values[2], values[3]);
          //  cout<<values[0][0]<<" "<<values[1][0]<<" "<<values[2][0]<<" "<<values[3][0]<<endl;
            
            std::vector<pack_t> second{ { a[lo+16], a[lo+17], a[lo+18], a[lo+19] },
                                        { a[lo+20], a[lo+21], a[lo+22], a[lo+23] },
                                        { a[lo+24], a[lo+25], a[lo+26], a[lo+27] },
                                        { a[lo+28], a[lo+29], a[lo+30], a[lo+31] } };
          
            tie(second[0], second[1], second[2], second[3]) =  floki::detail::bitonic_sort_16(second[0], second[1],
                                  second[2], second[3]);
            for(int i=0;i<32;i++) {
              if(i<32) a[i]=values[i/4][i%4];
              else a[i]=second[(i-16)/4][(i-16)%4];
            }
        //    cout<<second[0][0]<<" "<<second[1][0]<<" "<<second[2][0]<<" "<<second[3][0]<<endl;

            //reverse second
           
        }*/

/*
        if(cnt==16) {
          // cout<<"inside again."<<endl;
           using pack_t = pack<int32_t, 4>;
            std::vector<pack_t> values{ { a[lo+0], a[lo+1], a[lo+2], a[lo+3] },
                                        { a[lo+4], a[lo+5], a[lo+6], a[lo+7] } };
           //sort using bitonic sort
            tie(values[0], values[1]) = floki::detail::merge_sorted_vectors(values[0], values[1]);
          //  cout<<values[0][0]<<" "<<values[1][0]<<" "<<values[2][0]<<" "<<values[3][0]<<endl;
            
            std::vector<pack_t> second{ { a[lo+8], a[lo+9], a[lo+10], a[lo+11] },
                                        { a[lo+12], a[lo+13], a[lo+14], a[lo+15] }};
          
            tie(second[0], second[1]) =  floki::detail::merge_sorted_vectors(second[0], second[1]);
            for(int i=0;i<16;i++) {
              if(i<16) a[i]=values[i/4][i%4];
              else a[i]=second[(i-8)/4][(i-16)%4];
            }
        //    cout<<second[0][0]<<" "<<second[1][0]<<" "<<second[2][0]<<" "<<second[3][0]<<endl;

            //reverse second
           
        }*/

        /*if(cnt==8) {
          // cout<<"inside again."<<endl;
           using pack_t = pack<int32_t, 4>;
            std::vector<pack_t> values{ { a[lo+0], a[lo+1], a[lo+2], a[lo+3] },
                                        { a[lo+4], a[lo+5], a[lo+6], a[lo+7] } };
           //sort using bitonic sort
            tie(values[0], values[1]) = floki::detail::merge_sorted_vectors(values[0], values[1]);
          //  cout<<values[0][0]<<" "<<values[1][0]<<" "<<values[2][0]<<" "<<values[3][0]<<endl;
            
           
            for(int i=0;i<8;i++) {
               a[i]=values[i/4][i%4];
           
            }
        //    cout<<second[0][0]<<" "<<second[1][0]<<" "<<second[2][0]<<" "<<second[3][0]<<endl;

            //reverse second
           
        }*/

        else{
            sarg arg1;
            pthread_t thread1;
            arg1.lo = lo;
            arg1.cnt = k;
            arg1.dir = ASCENDING;
            arg1.layer = layer + 1;
            pthread_create( &thread1, NULL, PrecBitonicSort, &arg1 );
            
            sarg arg2;
            pthread_t thread2;
            arg2.lo = lo + k;
            arg2.cnt = k;
            arg2.dir = DESCENDING;
            arg2.layer = layer + 1;
            pthread_create( &thread2, NULL, PrecBitonicSort, &arg2 );
            
            
            pthread_join( thread1, NULL );
            pthread_join( thread2, NULL );
        }
        sarg arg3;
        arg3.lo = lo;
        arg3.cnt = cnt;
        arg3.dir = dir;
        arg3.layer = threadlayers - layer;
        PbitonicMerge( &arg3 );
    }
    return 0;
}


void * PrecBitonicSortB( void * arg ){
    int lo = ( ( sarg * ) arg ) -> lo;
    int cnt = ( ( sarg * ) arg ) -> cnt;
    int dir = ( ( sarg * ) arg ) -> dir;
    int layer = ( ( sarg * ) arg ) -> layer;

    if ( cnt > 1 ) {
        int k = cnt / 2;
        if( layer >= threadlayers ) {
            qsort( b + lo, k, sizeof( int ), asc );
            qsort( b + ( lo + k ) , k, sizeof( int ), desc );
        }
       // cout<<"K value: "<<k<<endl;
       /* if(cnt==32) {
          // cout<<"inside again."<<endl;
           using pack_t = pack<int32_t, 4>;
            std::vector<pack_t> values{ { a[lo+0], a[lo+1], a[lo+2], a[lo+3] },
                                        { a[lo+4], a[lo+5], a[lo+6], a[lo+7] },
                                        { a[lo+8], a[lo+9], a[lo+10], a[lo+11] },
                                        { a[lo+12], a[lo+13], a[lo+14], a[lo+15] } };
           //sort using bitonic sort
            tie(values[0], values[1], values[2], values[3]) = floki::detail::bitonic_sort_16(values[0], values[1],
                                                 values[2], values[3]);
          //  cout<<values[0][0]<<" "<<values[1][0]<<" "<<values[2][0]<<" "<<values[3][0]<<endl;
            
            std::vector<pack_t> second{ { a[lo+16], a[lo+17], a[lo+18], a[lo+19] },
                                        { a[lo+20], a[lo+21], a[lo+22], a[lo+23] },
                                        { a[lo+24], a[lo+25], a[lo+26], a[lo+27] },
                                        { a[lo+28], a[lo+29], a[lo+30], a[lo+31] } };
          
            tie(second[0], second[1], second[2], second[3]) =  floki::detail::bitonic_sort_16(second[0], second[1],
                                  second[2], second[3]);
            for(int i=0;i<32;i++) {
              if(i<32) a[i]=values[i/4][i%4];
              else a[i]=second[(i-16)/4][(i-16)%4];
            }
        //    cout<<second[0][0]<<" "<<second[1][0]<<" "<<second[2][0]<<" "<<second[3][0]<<endl;

            //reverse second
           
        }*/

/*
        if(cnt==16) {
          // cout<<"inside again."<<endl;
           using pack_t = pack<int32_t, 4>;
            std::vector<pack_t> values{ { a[lo+0], a[lo+1], a[lo+2], a[lo+3] },
                                        { a[lo+4], a[lo+5], a[lo+6], a[lo+7] } };
           //sort using bitonic sort
            tie(values[0], values[1]) = floki::detail::merge_sorted_vectors(values[0], values[1]);
          //  cout<<values[0][0]<<" "<<values[1][0]<<" "<<values[2][0]<<" "<<values[3][0]<<endl;
            
            std::vector<pack_t> second{ { a[lo+8], a[lo+9], a[lo+10], a[lo+11] },
                                        { a[lo+12], a[lo+13], a[lo+14], a[lo+15] }};
          
            tie(second[0], second[1]) =  floki::detail::merge_sorted_vectors(second[0], second[1]);
            for(int i=0;i<16;i++) {
              if(i<16) a[i]=values[i/4][i%4];
              else a[i]=second[(i-8)/4][(i-16)%4];
            }
        //    cout<<second[0][0]<<" "<<second[1][0]<<" "<<second[2][0]<<" "<<second[3][0]<<endl;

            //reverse second
           
        }*/

        /*if(cnt==8) {
          // cout<<"inside again."<<endl;
           using pack_t = pack<int32_t, 4>;
            std::vector<pack_t> values{ { a[lo+0], a[lo+1], a[lo+2], a[lo+3] },
                                        { a[lo+4], a[lo+5], a[lo+6], a[lo+7] } };
           //sort using bitonic sort
            tie(values[0], values[1]) = floki::detail::merge_sorted_vectors(values[0], values[1]);
          //  cout<<values[0][0]<<" "<<values[1][0]<<" "<<values[2][0]<<" "<<values[3][0]<<endl;
            
           
            for(int i=0;i<8;i++) {
               a[i]=values[i/4][i%4];
           
            }
        //    cout<<second[0][0]<<" "<<second[1][0]<<" "<<second[2][0]<<" "<<second[3][0]<<endl;

            //reverse second
           
        }*/

        else{
            sarg arg1;
            pthread_t thread1;
            arg1.lo = lo;
            arg1.cnt = k;
            arg1.dir = ASCENDING;
            arg1.layer = layer + 1;
            pthread_create( &thread1, NULL, PrecBitonicSortB, &arg1 );
            
            sarg arg2;
            pthread_t thread2;
            arg2.lo = lo + k;
            arg2.cnt = k;
            arg2.dir = DESCENDING;
            arg2.layer = layer + 1;
            pthread_create( &thread2, NULL, PrecBitonicSortB, &arg2 );
            
            
            pthread_join( thread1, NULL );
            pthread_join( thread2, NULL );
        }
        sarg arg3;
        arg3.lo = lo;
        arg3.cnt = cnt;
        arg3.dir = dir;
        arg3.layer = threadlayers - layer;
        PbitonicMergeB( &arg3 );
    }
    return 0;
}


void Psort() {
unsigned long long start_cycles = rdtsc(); //1
    sarg arg, arg2;
    arg.lo = 0;
    arg.cnt = N;
    arg.dir = ASCENDING;
    arg.layer = 0;

    
    PrecBitonicSort( &arg );
   
    unsigned long long end_first_cycles = rdtsc(); 
    arg2.lo = 0;
    arg2.cnt = M;
    arg2.dir = ASCENDING;
    arg2.layer = 0;

    PrecBitonicSortB( &arg2 );
    //printA();
    //printB();

    unsigned long long end_second_cycles = rdtsc(); 
    cout<<"Sort A Cycles: "<<(unsigned int)(end_first_cycles - start_cycles)<<endl;
    cout<<"Sort B Cycles: "<<(unsigned int)(end_second_cycles - end_first_cycles)<<endl;
    
    unsigned long long start_merge_cycles = rdtsc(); 
    //merging step
    int i=0,j = 0;
    while(i<N && j<M) {
       if (a[i]<b[j]) i++;
       else if (b[j]<a[i]) j++;
       else {
         int temp = a[i];
         vector<int> aindices;
         vector<int> bindices;
         while(i<N && a[i]==temp) {
           aindices.push_back(a[i]);
           i++;
         }
         while(j<M && b[j]==temp) {
           bindices.push_back(b[j]);
           j++;
         }
         //each a merged with all of j
         if(atoi(displayOutput)==1) {
         for(int k=0;k<aindices.size();k++)
          {  cout<<aindices[k]<<" ---> ";
              for(int l=0;l<bindices.size();l++)
                   cout<<bindices[l]<<" ";
             cout<<endl;
          }
        }
        
       } 
    }
    unsigned long long end_merge_cycles = rdtsc(); 
    cout<<"Merge Cycles: "<<(unsigned int)(end_merge_cycles - start_merge_cycles)<<endl;
 }




