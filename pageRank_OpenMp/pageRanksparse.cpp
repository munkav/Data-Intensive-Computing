#include <iostream>
#include <set>
#include <string>
#include <fstream>
#include <cstring> // can i do it without using it ?
#include <cmath>
#include <omp.h>
#include <iomanip>
#include <vector>
#define NUM_THREADS 4
#define ENABLE_OPENMP 
using namespace std;

class pageRank
{    
    int MAX_NODES;   // Is making static a good idea ?
    static const double CONVERGENCEVALUE = .000000001;
	vector<vector<pair<int,double> > > adj_mat;
    double *pageRankVector;
    const static double DAMPING_fACTOR = 0.85;
    int *outlink_count;

    void initalizeMatrices(); 
    void initpageRankMatrix();
    void normalizeColumns();
    

public:
    pageRank()
    {
    }
	
	void readFileAndGenMatrix();
    void writeMatrix();
    void calculatePageRank();


    ~pageRank()
    {
      //  delete(adj_mat);
        delete(pageRankVector);
        delete(outlink_count);
    }
		
};
/*
*
*
*/
void pageRank::readFileAndGenMatrix()
{

    ifstream infile;
    int source_node,destination_node;
    infile.open ("facebook_combined.txt", ifstream::in);
    pair <int,double> edge;
    set<int> countNodes;

    while(infile>>source_node>>destination_node)
    {
        countNodes.insert(source_node);
        countNodes.insert(destination_node);
    }

    MAX_NODES = countNodes.size();
    cout<<"total Pages "<< MAX_NODES;
    initalizeMatrices();
   
    infile.clear() ;
    infile.seekg(0, ios::beg) ;

    while(infile>>source_node>>destination_node)
    {
        edge = make_pair(destination_node, 1.0);
    	adj_mat[source_node].push_back(edge);
        edge = make_pair(source_node,1.0);
        adj_mat[destination_node].push_back(edge);
        outlink_count[source_node]++;
        outlink_count[destination_node]++;   // Counting outLinks of each Page.

    }

    infile.close();
    normalizeColumns();
    initpageRankMatrix();

}

void pageRank::initalizeMatrices()
{
    adj_mat.resize(MAX_NODES);
    for(int i = 0; i < MAX_NODES; i++)
    {
        adj_mat.push_back(vector<pair<int, double> >());
    }    

    pageRankVector = new double[MAX_NODES];
    memset(pageRankVector,0,sizeof(double)*MAX_NODES);
    outlink_count = new int[MAX_NODES];
    memset(outlink_count,0,sizeof(int)*MAX_NODES);

}
void pageRank::initpageRankMatrix()
{
    double initialPagerank = 1.0/MAX_NODES;
    cout<<"initial page rank "<<initialPagerank;
    cout<<MAX_NODES<<endl;
    #pragma omp parallel for
    for(int i = 0; i < MAX_NODES; i++)
    {
        pageRankVector[i] = initialPagerank;
    }
}

void pageRank::normalizeColumns()
{   
    double normalizedValue = 0.0;
    #pragma omp parallel 
    {
        double normalizedValue = 0.0;
        pair<int , double>  edge;
        #pragma omp for
        for(int i = 0; i < MAX_NODES; i++)
        {   
         
            for(int j = 0; j < adj_mat[i].size(); j++)
            {           
                adj_mat[i][j].second = 1.0/outlink_count[adj_mat[i][j].first];    
            }
        }
    }
}

void pageRank::writeMatrix()
{   
    double sum;
   /*for(int i = 0; i < MAX_NODES; i++)
    {
        sum = 0.0;
        for(int j = 0; j < adj_mat[i].size; j++)
            sum += adj_mat[j][i];
        cout <<endl;
        cout<<"sum of column "<<i<<" "<<setprecision(20)<<sum<<endl;
    }*/


    /*for(int i = 0; i < MAX_NODES; i++)
        cout<<outlink_count[i]<<endl;
    cout<<"end";*/

    ofstream ofile;
    ofile.open("Output_Task1.txt");
    for(int i = 0; i < MAX_NODES; i++)
    {
        ofile<<setprecision(20)<<i<<"  "<<pageRankVector[i]<<endl;
    }
    
}

void pageRank::calculatePageRank()
{
    
    bool notconverged = true;
    int count = 12;
    double * tempPageRankVector = new double[MAX_NODES];
    double TELEPORT_FACTOR = (1.0 - DAMPING_fACTOR)/MAX_NODES;
        
    notconverged = true;
    double  tempPageRank;
    while(notconverged)
    {

            notconverged = false;   
            memset(tempPageRankVector, 0, sizeof(double)*MAX_NODES);

            #pragma omp parallel 
            {   
                
                #pragma omp for reduction(||:notconverged)   //used notconverged as reduction we need to see if any one of 
                                                            // the parallel thread has converged or not. Even if one does not 
                                                            // converge, we need to continue the process
                for(int i = 0; i < MAX_NODES; i++)
                {   

                    for(int j = 0; j < adj_mat[i].size(); j++)
                    {   
                        tempPageRankVector[i] += adj_mat[i][j].second*pageRankVector[adj_mat[i][j].first];
                    }

                    tempPageRankVector[i] = tempPageRankVector[i]*DAMPING_fACTOR + TELEPORT_FACTOR;

                    if(fabs(tempPageRankVector[i] - pageRankVector[i]) >= CONVERGENCEVALUE)
                    {
                        notconverged = true;
                    }
                    
                }
            }

            copy(tempPageRankVector, tempPageRankVector + MAX_NODES, pageRankVector);

    }
      
}


int main()
{
    omp_set_num_threads(NUM_THREADS);
	pageRank *p = new pageRank();
    double start = omp_get_wtime();
	p->readFileAndGenMatrix();
    p->calculatePageRank();
    cout<<omp_get_wtime() - start<<endl;
    p->writeMatrix();
    return 0;
}