/*
    Copyright 2011, Spyros Blanas.

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "algo.h"
#include <algorithm>
 #include <iostream>
#include <iomanip>
#include <vector>
#include <map>
 #include <queue>
#ifdef VERBOSE

using namespace std;
#endif

struct SortedDataInfo {
	map<long long, vector<void *> > keyToTuples;
	vector<long long> keys; 
};

void StoreCopy::init(
		Schema* schema1, vector<unsigned int> select1, unsigned int jattr1,
		Schema* schema2, vector<unsigned int> select2, unsigned int jattr2) {
	HashBase::init(schema1, select1, jattr1, schema2, select2, jattr2);

	// create hashtable with new build schema
	for(int i=0;i<nthreads;i++) {
		hashtables[i].init(_hashfn->buckets(), size, sbuild->getTupleSize());
	}
	hashtable.init(_hashfn->buckets(), size, sbuild->getTupleSize());
}

void StoreCopy::destroy() 
{
	HashBase::destroy();

	hashtable.destroy();
}

void StoreCopy::buildCursor(PageCursor* t, int threadid, bool atomic)
{
	if (atomic)
		realbuildCursor<true>(t, threadid);
	else
		realbuildCursor<false>(t, threadid);
}


SortedDataInfo getSortedDataInfoForPages(PageCursor* t, int ja2, int threadid) {
	void* tup;
	Page* b;
	//cout<<"start of sortdatainfopages function in threadid "<<threadid<<endl<<flush<<endl;
	Schema* s = t->schema();
    SortedDataInfo ret;
	vector<long long> keys;
	map<long long, vector<void *> > keyToTuple;
	int i = 0;
	int iter=0;
    int priorcount=0;
  
	while(b = t->readNext()) {
		i = 0;
		while(tup = b->getTupleOffset(i++)) {
		  long long key = s->asLong(tup, ja2);
//          if(key==793973) priorcount++;

          if(keyToTuple.find(key) == keyToTuple.end()) {
          	  vector<void *> temp;
          	  temp.push_back(tup);
          	  keyToTuple[key] = temp;
		      keys.push_back(key);
          } else {
          	  keyToTuple[key].push_back(tup);
          }
          iter++;
        }
    }
   // cout<<threadid<<" "<<iter<<endl;
    std::sort(keys.begin(), keys.end());
    ret.keys = keys;
    ret.keyToTuples = keyToTuple;
  //  cout<<"end of sortdatainfopages function in thread "<<threadid<<endl<<flush<<endl;
    return ret;

}


template <bool atomic>
void StoreCopy::realbuildCursor(PageCursor* t, int threadid)
{
  //cout<<"start of realbuildcursorfunction "<<flush<<endl;	

  Schema* s = t->schema();	
  SortedDataInfo sortResult = getSortedDataInfoForPages(t, ja2, threadid);
  vector<long long> keys = sortResult.keys;
  map<long long, vector<void *> >keyToTuple = sortResult.keyToTuples;

//cout<<"creating hashtable for thread "<<threadid<<flush<<endl;
  
   //cout<<"ending hashtable for thread "<<threadid<<flush<<endl; 
    int iter = 0;
    for(int keyiter=0;keyiter<keys.size();keyiter++) {
    	//cout<<"starting key number "<<keyiter<<" in threadid "<<threadid<<endl<<flush;
    	vector<void *> tuples = keyToTuple[keys[keyiter]];
    	//cout<<keys[j]<<"\n";
    	for(int i=0;i<tuples.size();i++) {
    		iter++;
    		void *tup = tuples[i];
    		void* target = hashtables[threadid].allocate(0);

    		sbuild->writeData(target, 0, s->calcOffset(tup, ja2));
    		
			for (unsigned int j=0; j<sel2.size(); ++j)
				sbuild->writeData(target,		// dest
						j+1,	// col in output
						s->calcOffset(tup, sel2[j]));	// src for this col


    	}
    //	cout<<"Ending key number "<<keyiter<<" in threadid "<<threadid<<endl<<flush;
    }
    //cout<<"end of realbuildcursorfunction for thread "<<threadid<<endl<<flush<<endl;	

    /*
    for(int i = 0; i < keys.size(); i++) {
    	long long sKey = keys[i];
    	cout<<"Thread ID"<<i<<"special key"<<sKey<<endl<<flush;
    }
	
	HashTable::Iterator it = hashtables[threadid].createIterator();
	hashtables[threadid].placeIterator(it, 0);
	void *sTup = it.readnext();
    while(sTup != NULL) {
    	long long sKey = sbuild->asLong(sTup, 0);
    	cout<<"THREAD ID"<< threadid <<"ITER special key"<<sKey<<endl<<flush;
    	sTup = it.readnext();
    }

 */

}
     
	    

	/*
	
	unsigned int curbuc;
	while(b = (atomic ? t->atomicReadNext() : t->readNext())) {
		i = 0;
		while(tup = b->getTupleOffset(i++)) {
			// find hash table to append
			curbuc = _hashfn->hash(s->asLong(tup, ja1));
			void* target = atomic ? 
				hashtable.atomicAllocate(curbuc) :
				hashtable.allocate(curbuc);

#ifdef VERBOSE
		cout << "Adding tuple with key " 
			<< setfill('0') << setw(7) << s->asLong(tup, ja1)
			<< " to bucket " << setfill('0') << setw(4) << curbuc << endl;
#endifke

			sbuild->writeData(target, 0, s->calcOffset(tup, ja1));
			for (unsigned int j=0; j<sel1.size(); ++j)
				sbuild->writeData(target,		// dest
						j+1,	// col in output
						s->calcOffset(tup, sel1[j]));	// src for this col

		}
	}
}*/

WriteTable* StoreCopy::probeCursor(PageCursor* t, int threadid, bool atomic, WriteTable* ret)
{
	if (atomic)
		return realprobeCursor<true>(t, threadid, ret);
	return realprobeCursor<false>(t, threadid, ret);
}

template <bool atomic>
WriteTable* StoreCopy::realprobeCursor(PageCursor* t, int threadid, WriteTable* ret)
{
	//cout<<"starting real proble for thread "<<threadid<<endl<<flush;
	 if(ret == NULL) {
		ret = new WriteTable();
		ret->init(sout, outputsize);
	}
    //cout<<"starting execution real proble for thread "<<threadid<<endl<<flush;
	Schema* s = t->schema();	
    SortedDataInfo sortResult = getSortedDataInfoForPages(t, ja1, threadid);
    vector<long long> keys = sortResult.keys;
    map<long long, vector<void *> >keyToTuples = sortResult.keyToTuples; 
    /*
    if(threadid == 0) {
		for(int i = 0; i < nthreads; i++) {
	    	HashTable::Iterator it = hashtables[i].createIterator();
	    	hashtables[i].placeIterator(it, 0);
	    	void *sTup = it.readnext();
	    	while(sTup != NULL) {
		    	long long sKey = sbuild->asLong(sTup, 0);
	    		cout<<"Thread ID"<< i <<"s key"<<sKey<<endl<<flush;
	    		sTup = it.readnext();
	    	}
	    }

	    for(int i = 0; i < keys.size(); i++) {
	    	long long rKey = keys[i];
	    	cout<<"Thread ID"<<i<<"r key"<<rKey<<endl<<flush;
	    }
    }

*/

    int keyiter = 0, tupleiter=0;
    HashTable::Iterator iter[nthreads];
    //iter = new HashTable::Iterator[nthreads];
    /*
    cout<<"no of threads"<<nthreads<<endl<<flush;
    //cout<<"starting real proble hashtables init for thread "<<threadid<<endl<<flush;
    */
    for(int i=0;i<nthreads;i++) {
    	iter[i] = hashtables[i].createIterator();
    	hashtables[i].placeIterator(iter[i],0);
    	//iter[i] = &it;
    }
    /*
    //cout<<threadid<<" keysize"<<keys.size()<<endl<<flush;
    cout<<"finished hashtable loop real proble for thread "<<threadid<<endl<<flush;
*/
    HashTable::Iterator it = hashtables[threadid].createIterator();
    hashtables[threadid].placeIterator(it, 0);
    //Create an empty space of nothreads hashtable pointers and then each thread will write its pointer to sorted S there
/*
    void* tup;
    if(threadid == 0) {
    	for(int i = 0; i < 12; i++) {
    		int ite = 0;
    		int ctr = 0;
    		while(tup = iter[i].readnext()) {
		    	//if(iter < 5 && threadid == 0)
		    		//cout<<threadid<<":::::"<<sbuild->prettyprint(tup, '\t')<<endl;
		    	ite++;
		    	long long key = sbuild->asLong(tup, 0);
		    	if(key == 793973) {
		    		ctr++;
		    	}
		    }
	    	cout<<threadid<<"-"<<ite<<endl<<flush;
	    	cout<<"Number of 793973: "<<ctr<<endl<<flush;
    	}
		    
    }
    
*/
     //cout<<"ending real proble hashtables init for thread "<<threadid<<endl<<flush;

    //S
    
   // cout<<"starting real proble adding elts to queue init for thread "<<threadid<<endl<<flush;
    priority_queue<pair<long long, pair<void *, int> >, vector<pair<long long, pair<void*, int> > >/*, greater< pair<long long, pair<void *, int> > >*/ > pq;
    for(int i=0;i<nthreads;i++) {
    	//cout<<"about to access iter["<<i<<"].readnext()"<<endl<<flush;
    	void* tup = iter[i].readnext();
    	if(tup != NULL) {
    		long long key = sbuild->asLong(tup, 0);
    		//cout<<"obtained key"<<flush;
    		pq.push(make_pair(key, make_pair(tup, i)));	
    		//cout<<"added to pq initial"<<flush;
    	}
    }
   // cout<<"ending real proble adding elts to queue init for thread "<<threadid<<endl<<flush;
   // cout<<"queue size initial (should be 12) "<<pq.size()<<endl<<flush;
     int counteltsinr = 0;
     int counteltsins = 0;

    //R
    int rKeyIter = keys.size() - 1, rTupIndex=0;
    int te = 0;
    int joinentries = 0;

    //cout<<threadid<<"-"<<"minSKey: "<< pq.top().first<<"minRKey: "<<keys[0]<<"maxRKey: "<<keys[keys.size()-1];
    while(rKeyIter >= 0 && !pq.empty()) {
    	//cout<<"MAIN COUNTS: "<<rKeyIter<<" "<<keys.size()<<" "<<pq.size()<<endl<<flush;
    	pair<long long, pair<void *, int> > ele;
    	ele = pq.top();
    	long long skey = ele.first;
    	void* stup, *rtup;
    	HashTable::Iterator it;
    	long long rkey = keys[rKeyIter];
    	//cout<<threadid<<" "<<skey<<" "<<rkey<<endl<<flush;
    	//if(te < 5) {
    	//cout<<skey<<" "<<rkey<<endl<<flush;
    	te++;
    	//}
    	//cout<<"check 1"<<endl<<flush;
        if(skey == rkey) {
        	//cout<<"equal \n"<<flush;
        	vector<void *> rtuples = keyToTuples[rkey];
        	counteltsinr += rtuples.size();
        	vector<void *> stuples;
        	
            while(!pq.empty() && ele.first == rkey) {
            	//cout<<"entering  inner while "<<pq.size()<<" thread"<<threadid<<endl<<flush;
            	stup = ele.second.first;
            	stuples.push_back(stup);
            	cout<<"Poping" << skey<<endl<<flush;
            	pq.pop();
            	counteltsins++;
            	stup = iter[ele.second.second].readnext();
            	if(stup) {
        	    	pq.push(make_pair(sbuild->asLong(stup, 0), make_pair(stup, ele.second.second)));
            	}
            	if(!pq.empty())
            		ele = pq.top();
            }

            //stuples.push_back(stup);
            //pq.pop();	
            stup = iter[ele.second.second].readnext();
            if(stup) {
        	    pq.push(make_pair(sbuild->asLong(stup, 0), make_pair(stup, ele.second.second)));
            }
            cout<<"Stuples with key " << rkey<<stuples.size()<<endl<<flush;
            joinentries += stuples.size() * rtuples.size();
            //cout<<"exit inner while"<<endl<<flush;
        	if(s2->getTupleSize()) {
        		for(int i = 0; i < rtuples.size(); i++) {
        			rtup = rtuples[i];
        			for(int j = 0; j < stuples.size(); j++) {
        				stup = stuples[j];
        				char tmp[sout->getTupleSize()];
						s2->copyTuple(tmp, sbuild->calcOffset(stup,1));
			        	for (unsigned int k=0; k<sel1.size(); ++k)
								sout->writeData(tmp,		// dest
										s2->columns()+k,	// col in output
										s1->calcOffset(rtup, sel1[k]));	// src for this col
			        	ret->append(tmp);
        			}
        		}
        	}
        	rKeyIter--;
            //cout<<"check 2"<<endl<<flush;
        } else if(rkey > skey) {
        	rKeyIter--;
        	counteltsinr++;
        } else {
        	//it = iter[ele.second.second];
        	stup = iter[ele.second.second].readnext();
        	pq.pop();
        	counteltsins++;
        	if(stup) {
    	    	pq.push(make_pair(sbuild->asLong(stup, 0), make_pair(stup, ele.second.second)));
        	}
        }
    }
    cout<<"Equal matches: "<<joinentries<<endl<<flush;
    /*
    cout<<"Total R in threead "<<threadid<<": "<<counteltsinr<<endl<<flush;
    cout<<"Total S in threead "<<threadid<<": "<<counteltsins<<endl<<flush;
    */
    return ret;
 
}



    


	/*if (ret == NULL) {
		ret = new WriteTable();
		ret->init(sout, outputsize);
	}

	char tmp[sout->getTupleSize()];
	void* tup1;
	void* tup2;
	Page* b2;
	unsigned int curbuc, i;

	HashTable::Iterator it = hashtable.createIterator();

	while (b2 = (atomic ? t->atomicReadNext() : t->readNext())) {
#ifdef VERBOSE
		cout << "Working on page " << b2 << endl;
#endif
		i = 0;
		while (tup2 = b2->getTupleOffset(i++)) {
#ifdef VERBOSE
			cout << "Joining tuple " << b2 << ":" 
				<< setfill('0') << setw(6) << i 
				<< " having key " << s2->asLong(tup2, ja2) << endl;
#endif
			curbuc = _hashfn->hash(s2->asLong(tup2, ja2));
#ifdef VERBOSE
			cout << "\twith bucket " << setfill('0') << setw(6) << curbuc << endl;
#endif
			hashtable.placeIterator(it, curbuc);

#ifdef PREFETCH
#warning Only works for 16-byte tuples!
			hashtable.prefetch(_hashfn->hash(
						*(unsigned long long*)(((char*)tup2)+32)
						));
			hashtable.prefetch(_hashfn->hash(
						*(unsigned long long*)(((char*)tup2)+64)
						));
#endif

			while (tup1 = it.readnext()) {
				if (sbuild->asLong(tup1,0) != s2->asLong(tup2,ja2) ) {
					continue;
				}

#if defined(OUTPUT_ASSEMBLE)
				// copy payload of first tuple to destination
				if (s1->getTupleSize()) 
					s1->copyTuple(tmp, sbuild->calcOffset(tup1,1));

				// copy each column to destination
				for (unsigned int j=0; j<sel2.size(); ++j)
					sout->writeData(tmp,		// dest
							s1->columns()+j,	// col in output
							s2->calcOffset(tup2, sel2[j]));	// src for this col
#if defined(OUTPUT_WRITE_NORMAL)
				ret->append(tmp);
#elif defined(OUTPUT_WRITE_NT)
				ret->nontemporalappend16(tmp);
#endif
#endif

#if defined(OUTPUT_AGGREGATE)
				aggregator[ (threadid * AGGLEN) +
					+ (sbuild->asLong(tup1,0) & (AGGLEN-1)) ]++;
#endif

#if !defined(OUTPUT_AGGREGATE) && !defined(OUTPUT_ASSEMBLE)
				__asm__ __volatile__ ("nop");
#endif

			}
		}
	}
	return ret;
	*/





void StorePointer::init(
		Schema* schema1, vector<unsigned int> select1, unsigned int jattr1,
		Schema* schema2, vector<unsigned int> select2, unsigned int jattr2) {
	HashBase::init(schema1, select1, jattr1, schema2, select2, jattr2);

	// override sbuild, add s1
	s1 = schema1;
	delete sbuild;

	// generate build schema
	sbuild = new Schema();
	sbuild->add(schema1->get(ja1));
	sbuild->add(CT_POINTER);
	
	// create hashtable with new build schema
	hashtable.init(_hashfn->buckets(), size, sbuild->getTupleSize());
}

void StorePointer::buildCursor(PageCursor* t, int threadid, bool atomic)
{
	if (atomic)
		realbuildCursor<true>(t, threadid);
	else
		realbuildCursor<false>(t, threadid);
}

template <bool atomic>
void StorePointer::realbuildCursor(PageCursor* t, int threadid)
{
	int i = 0;
	void* tup;
	Page* b;
	Schema* s = t->schema();
	unsigned int curbuc;
	while(b = (atomic ? t->atomicReadNext() : t->readNext())) {
		i = 0;
		while(tup = b->getTupleOffset(i++)) {
			// find hash table to append
			curbuc = _hashfn->hash(s->asLong(tup, ja1));
			void* target = atomic ?
				hashtable.atomicAllocate(curbuc) :
				hashtable.allocate(curbuc);

#ifdef VERBOSE
		cout << "Adding tuple with key " 
			<< setfill('0') << setw(7) << s->asLong(tup, ja1)
			<< " to bucket " << setfill('0') << setw(4) << curbuc << endl;
#endif

			sbuild->writeData(target, 0, s->calcOffset(tup, ja1));
			sbuild->writeData(target, 1, &tup);

		}
	}
}

WriteTable* StorePointer::probeCursor(PageCursor* t, int threadid, bool atomic, WriteTable* ret)
{
	if (atomic)
		return realprobeCursor<true>(t, threadid, ret);
	return realprobeCursor<false>(t, threadid, ret);
}

template <bool atomic>
WriteTable* StorePointer::realprobeCursor(PageCursor* t, int threadid, WriteTable* ret)
{
	if (ret == NULL) {
		ret = new WriteTable();
		ret->init(sout, outputsize);
	}

	char tmp[sout->getTupleSize()];
	void* tup1;
	void* tup2;
	Page* b2;
	unsigned int curbuc, i;

	HashTable::Iterator it = hashtable.createIterator();

	while (b2 = (atomic ? t->atomicReadNext() : t->readNext())) {
		i = 0;
		while (tup2 = b2->getTupleOffset(i++)) {
			curbuc = _hashfn->hash(s2->asLong(tup2, ja2));
			hashtable.placeIterator(it, curbuc);

#ifdef PREFETCH
#warning Only works for 16-byte tuples!
			hashtable.prefetch(_hashfn->hash(
						*(unsigned long long*)(((char*)tup2)+32)
						));
			hashtable.prefetch(_hashfn->hash(
						*(unsigned long long*)(((char*)tup2)+64)
						));
#endif

			while (tup1 = it.readnext()) {
				if (sbuild->asLong(tup1,0) != s2->asLong(tup2,ja2) ) {
					continue;
				}

#if defined(OUTPUT_ASSEMBLE)
				void* realtup1 = sbuild->asPointer(tup1, 1);
				// copy each column to destination
				for (unsigned int j=0; j<sel1.size(); ++j)
					sout->writeData(tmp,		// dest
							j,		// col in output
							s1->calcOffset(realtup1, sel1[j]));	// src for this col
				for (unsigned int j=0; j<sel2.size(); ++j)
					sout->writeData(tmp,		// dest
							sel1.size()+j,	// col in output
							s2->calcOffset(tup2, sel2[j]));	// src for this col
#if defined(OUTPUT_WRITE_NORMAL)
				ret->append(tmp);
#elif defined(OUTPUT_WRITE_NT)
				ret->nontemporalappend16(tmp);
#endif
#endif

#if defined(OUTPUT_AGGREGATE)
				aggregator[ (threadid * AGGLEN) +
					+ (sbuild->asLong(tup1,0) & (AGGLEN-1)) ]++;
#endif

#if !defined(OUTPUT_AGGREGATE) && !defined(OUTPUT_ASSEMBLE)
				__asm__ __volatile__ ("nop");
#endif

			}
		}
	}
	return ret;
}

void StorePointer::destroy() {
	// Cannot call HashBase::destroy(), as s1==sbuild for us and it will result
	// in a double free.
	
	// XXX memory leak: can't delete, pointed to by output tables
	// delete sout;
	delete sbuild;

	hashtable.destroy();
}

