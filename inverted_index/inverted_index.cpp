/* Copyright (c) 2007-2011, Stanford University
* All rights reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
*     * Redistributions of source code must retain the above copyright
*       notice, this list of conditions and the following disclaimer.
*     * Redistributions in binary form must reproduce the above copyright
*       notice, this list of conditions and the following disclaimer in the
*       documentation and/or other materials provided with the distribution.
*     * Neither the name of Stanford University nor the names of its 
*       contributors may be used to endorse or promote products derived from 
*       this software without specific prior written permission.
*
* THIS SOFTWARE IS PROVIDED BY STANFORD UNIVERSITY ``AS IS'' AND ANY
* EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
* WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
* DISCLAIMED. IN NO EVENT SHALL STANFORD UNIVERSITY BE LIABLE FOR ANY
* DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
* (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
* LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
* ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
* SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/ 

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <ctype.h>
#include <atomic>
#include <algorithm>

#include "map_reduce.h"
#define DEFAULT_DISP_NUM 10

// a passage from the text. The input data to the Map-Reduce
struct wc_string {
    char* data;
    uint64_t len;
};

// a single null-terminated word
struct wc_word {
    char* data;
    
    // necessary functions to use this as a key
    bool operator<(wc_word const& other) const {
        return strcmp(data, other.data) < 0;
    }
    bool operator==(wc_word const& other) const {
        return strcmp(data, other.data) == 0;
    }
};


// a hash for the word
struct wc_word_hash
{
    // FNV-1a hash for 64 bits
    size_t operator()(wc_word const& key) const
    {
        char* h = key.data;
        uint64_t v = 14695981039346656037ULL;
        while (*h != 0)
            v = (v ^ (size_t)(*(h++))) * 1099511628211ULL;
        return v;
    }
};

struct chunk_details{
    uint64_t chunk_no;
    uint64_t total_lines;
    uint64_t start;
    char* data;;
    bool operator<(chunk_details const& other) const {
    return (chunk_no < other.chunk_no);
    }
};

struct find_word{
    wc_word word;
    //int chunk_no[1];
    std::vector <uint64_t> line;
    std::vector <uint64_t> chunk_no;
};


class WordsMR : public MapReduceSort<WordsMR, wc_string, wc_word, uint64_t, hash_container<wc_word, uint64_t, sum_combiner, wc_word_hash> >
{
    char* data;
    uint64_t data_size;
    uint64_t chunk_size;
    uint64_t splitter_pos;
    std::vector <find_word> match;
    std::vector <chunk_details> chunk_status;
    std::vector <chunk_details> current_chunk;
    std::atomic<int> chunk_no_;
    uint64_t number_of_chunks;

public:
    explicit WordsMR(char* _data, uint64_t length, uint64_t _chunk_size) :
        data(_data), data_size(length), chunk_size(_chunk_size), 
            splitter_pos(0) {chunk_no_ = 0; number_of_chunks = 1;}

    void* locate(data_type* str, uint64_t len) const
    {
        return str->data;
    }
    void addWord(char word_[]){
        
        char *wordd = strdup(word_);
        int len = (int)strlen(wordd);
        if(word_ == NULL)return;

        for(uint64_t i = 0; i < len ; i++)wordd[i] = toupper(wordd[i]);

        find_word t;
        t.word = wc_word{wordd};
        match.push_back(t);
    }
    
    void map(data_type const& s, map_container& out) const
    {

        chunk_details temp;
        int index_;

        for(int i=0; i < current_chunk.size(); i++){
            if(current_chunk.at(i).data == s.data){
                temp = current_chunk.at(i);
                index_ = i;
            }
        }

        for (uint64_t i = 0; i < s.len; i++)
        {
            s.data[i] = toupper(s.data[i]);
        }

        uint64_t count_lines = 1;
        temp.total_lines = count_lines;

        uint64_t i = 0;
        while(i < s.len)
        {            
            while(i < s.len && (s.data[i] < 'A' || s.data[i] > 'Z')){
                i++;
                if(s.data[i] == '\n'){
                    count_lines++;
                    temp.total_lines = count_lines;   
                }
                
            }

            uint64_t start = i;
            while(i < s.len && ((s.data[i] >= 'A' && s.data[i] <= 'Z') || s.data[i] == '\''))
                i++;
            if(i > start)
            {
                s.data[i] = 0;
                wc_word word = { s.data+start };
                for(uint64_t k = 0; k < match.size(); k++){
                    if(word == match.at(k).word){

                        if(match.at(k).line.empty()){
                            match.at(k).line.push_back(count_lines+1- current_chunk.at(index_).chunk_no);
                            match.at(k).chunk_no.push_back(current_chunk.at(index_).chunk_no);
                        }
                        else if(count_lines>match.at(k).line.at(match.at(k).line.size()-1) && match.at(k).line.size()<=10){
                        match.at(k).line.push_back(count_lines+1- current_chunk.at(index_).chunk_no);
                        match.at(k).chunk_no.push_back(current_chunk.at(index_).chunk_no);
                        }
                    }
                }

            }
        }
        
        chunk_status.push_back(temp);

    }

    /** wordcount split()
     *  Memory map the file and divide file on a word border i.e. a space.
     */
    int split(wc_string& out)
    {
        
        /* End of data reached, return FALSE. */
        if ((uint64_t)splitter_pos >= data_size)
        {
            return 0;
        }

        chunk_details temp;
        //temp.start = splitter_pos;
        temp.chunk_no = number_of_chunks;

        /* Determine the nominal end point. */
        uint64_t end = std::min(splitter_pos + chunk_size, data_size);

        /* Move end point to next word break */
        while(end < data_size && 
            data[end] != ' ' && data[end] != '\t' &&
            data[end] != '\r' && data[end] != '\n')
            end++;

        /* Set the start of the next data. */
        out.data = data + splitter_pos;
        out.len = end - splitter_pos;

        temp.data = out.data;
        
        splitter_pos = end;

        current_chunk.push_back(temp);
        number_of_chunks++;

        /* Return true since the out data is valid. */
        return 1;
    }

    void display(){

        std::sort(chunk_status.begin(),chunk_status.end());

        for(uint64_t i = 0; i < match.size(); i++){
            printf(match.at(i).word.data );
            printf(" - Lines are :");

            for(uint64_t j = 0; j < match.at(i).line.size(); j++){

                if(match.at(i).chunk_no.at(j)<2)
                    printf(" %d",match.at(i).line.at(j) );

                 else if(match.at(i).chunk_no.at(j)>1){
                    int line_no_t = match.at(i).line.at(j);

                    for(int m = 0; m < match.at(i).chunk_no.at(j)-1; m++){
                        line_no_t += chunk_status.at(m).total_lines;
                    }
                    printf(" %d ",line_no_t );
                 }
            
            }
            printf("\n" );
        }
     }

};



int main(int argc, char *argv[]) 
{
    int fd;
    char * fdata;
    unsigned int disp_num;
    struct stat finfo;
    char * fname, * disp_num_str;
    struct timespec begin, end;

    get_time (begin);

    // Make sure a filename is specified
    if (argv[1] == NULL)
    {
        printf("USAGE: %s <filename> [Top # of results to display]\n", argv[0]);
        exit(1);
    }

    fname = argv[1];
    disp_num_str = argv[2];

    printf("Wordcount: Running...\n");

    // Read in the file
    CHECK_ERROR((fd = open(fname, O_RDONLY)) < 0);
    // Get the file info (for file length)
    CHECK_ERROR(fstat(fd, &finfo) < 0);

    uint64_t r = 0;

    fdata = (char *)malloc (finfo.st_size);
    CHECK_ERROR (fdata == NULL);
    while(r < (uint64_t)finfo.st_size)
        r += pread (fd, fdata + r, finfo.st_size, r);
    CHECK_ERROR (r != (uint64_t)finfo.st_size);

    
    // Get the number of results to display
    CHECK_ERROR((disp_num = (disp_num_str == NULL) ? 
      DEFAULT_DISP_NUM : atoi(disp_num_str)) <= 0);

    get_time (end);

    #ifdef TIMING
    print_time("initialize", begin, end);
    #endif

    printf("Wordcount: Calling MapReduce Scheduler Wordcount\n");
    get_time (begin);
    std::vector<WordsMR::keyval> result;    
    WordsMR mapReduce(fdata, finfo.st_size, 1024*1024);

    mapReduce.addWord("mabuza");
    mapReduce.addWord("Sherlock");
    CHECK_ERROR( mapReduce.run(result) < 0);
    get_time (end);

    #ifdef TIMING
    print_time("library", begin, end);
    #endif
    printf("Wordcount: MapReduce Completed\n");

    get_time (begin);

    unsigned int dn = std::min(disp_num, (unsigned int)result.size());
    printf("\nWordcount: Results (TOP %d of %lu):\n", dn, result.size());

    mapReduce.display();

    free (fdata);

    CHECK_ERROR(close(fd) < 0);

    get_time (end);

    #ifdef TIMING
    print_time("finalize", begin, end);
    #endif

    return 0;
}

// vim: ts=8 sw=4 sts=4 smarttab smartindent
