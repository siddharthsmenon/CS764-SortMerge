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
#include <utility>
using namespace std;

void BaseAlgo::init(
		Schema* schema1, vector<unsigned int> select1, unsigned int jattr1,
		Schema* schema2, vector<unsigned int> select2, unsigned int jattr2) {
	// copy to private space
	s1 = schema1;
	sel1 = select1;
	sel2 = select2;
	ja1 = jattr1;
	ja2 = jattr2;

	// generate output & build schema
	sout = new Schema();
	sbuild = new Schema();
	//s1 = new Schema();
	s2 = new Schema();

// ja2, sel2 in sbuild
	sbuild->add(schema2->get(ja2));
	
	
	for (vector<unsigned int>::iterator i2=sel2.begin(); i2!=sel2.end(); ++i2) {
		pair<ColumnType, unsigned int> ct = schema2->get(*i2);
		sout->add(ct);
		sbuild->add(ct);
		s2->add(ct);
	}

	for (vector<unsigned int>::iterator i1=sel1.begin(); i1!=sel1.end(); ++i1) {
		pair<ColumnType, unsigned int> ct = s1->get(*i1);
		sout->add(ct);
		//s1->add(ct);
	}

}

void BaseAlgo::destroy() {
	// XXX memory leak: can't delete, pointed to by output tables
	// delete sout;
	delete sbuild;
	delete s2;
}

BaseAlgo::BaseAlgo(const libconfig::Setting& cfg) {
}
