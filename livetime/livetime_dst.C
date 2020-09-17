#include <iostream>
#include <fstream>
#include "TFile.h"
#include "TH1D.h"

using namespace std;

int main(int argc, char* argv[]){
    string rmin = argv[1];
    string rmax = argv[2];
    cout << "Runs: " << rmin << " - " << rmax << endl;

    // read file with dst paths
    string fname = "livetime/dst_list" + rmin + "-" + rmax + ".list";
    cout << "Reading from: " << fname << endl;
    ifstream f(fname.c_str());

    // read livetime from each dst
    string dst;
    TH1D* hlive = new TH1D("livetime_tot", "livetime_tot", 30198, 5002.5, 35200.5);
    cout << "Adding histos..." << endl;
    while (f >> dst){
        // get livetime histogram
        cout << dst << endl;
        TFile* fdst = TFile::Open(dst.c_str());
        TH1D* h = (TH1D*) fdst->Get("live_time");
        // add them all together
        hlive->Add(h);

        fdst->Close();
    }
    f.close();

    // loop over runs and save info
    cout << "Getting livetime..." << endl;
    int nbins = hlive->GetXaxis()->GetNbins();
    fname = "livetime/livetime_run" + rmin + "-" + rmax + ".csv";
    ofstream out(fname.c_str());
    // header
    out << "RunNumber Livetime" << endl;
    for(int bin=1; bin < nbins + 1; bin++){
        float lt = hlive->GetBinContent(bin);
        // save only runs with non-zero livetime
        if(lt) out << hlive->GetBinCenter(bin) << " " << lt << endl;
    }
    cout << "--> " << fname << endl << endl;

}

