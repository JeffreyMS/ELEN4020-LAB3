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
#include <algorithm>

#include "map_reduce.h"
#define DEFAULT_DISP_NUM 50

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
    char* data;
    bool operator<(chunk_details const& other) const {
    return (chunk_no < other.chunk_no);
    }
};

struct find_word{
    wc_word word;
    std::vector <uint64_t> chunk_no;
};

struct value{
    uint64_t line_no;
    uint64_t chunk_no;
    std::vector <uint64_t> line;
    std::vector <uint64_t> cn;
    bool operator==(value const& other) const {
        return (line.at(cn.size()-1) == other.line.at(other.cn.size()-1)) && (cn.at(cn.size()-1) == other.cn.at(other.cn.size()-1));
    }
};

class WordsMR : public MapReduceSort<WordsMR, wc_string, wc_word, value, hash_container<wc_word, value, buffer_combiner, wc_word_hash> >
{
    char* data;
    uint64_t data_size;
    uint64_t chunk_size;
    uint64_t splitter_pos;
    std::vector <find_word> match;
    std::vector <chunk_details> chunk_status;
    std::vector <chunk_details> current_chunk;
    uint64_t number_of_chunks;

public:
    explicit WordsMR(char* _data, uint64_t length, uint64_t _chunk_size) :
        data(_data), data_size(length), chunk_size(_chunk_size), 
            splitter_pos(0) { number_of_chunks = 1;}

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
        uint64_t index_;

        for(uint64_t i=0; i < current_chunk.size(); i++){
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
   
                        value vl;
                        vl.line_no = count_lines;
                        vl.chunk_no = index_;
                        emit_intermediate(out, match.at(k).word, vl);

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

        //Keeps track of the file we processing
        chunk_details temp;
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

    /*************************************************************************/
    // Lets overload the reduce function
    void reduce(key_type const& key, reduce_iterator const& values, 
        std::vector<keyval>& out) const {
        value val;
 
        while (values.next(val))
        {
            val.line.push_back(val.line_no);
            val.cn.push_back(val.chunk_no);

            keyval kv = {key, val};
            out.push_back(kv);

            if(out.size()>1){
                if(out.at(out.size()-1).key == out.at(out.size()-2).key){
                    
                    //if the values of the key are the same, keep one for one value
                    if(!(out.at(out.size()-2).val == out.at(out.size()-1).val)
                    ){
                    out.at(out.size()-2).val.line.push_back(out.at(out.size()-1).val.line_no);
                    out.at(out.size()-2).val.cn.push_back(out.at(out.size()-1).val.chunk_no);
                    }
                    //All values of the same key are merged together
                    out.pop_back();
                    
                }
            }
            
        }

        
    }

void fix_arrange(std::vector<WordsMR::keyval> &result){
    std::sort(chunk_status.begin(),chunk_status.end());
    
    for (size_t i = 0; i < result.size(); i++)
    {
        //add line numbers from other chunks
        for (size_t j = 0; j < result[i].val.line.size(); j++){
            int summer = 0;
            for(int k = 0; k < result[i].val.cn.at(j); k++)  
                summer+=chunk_status.at(k).total_lines-1;

            result[i].val.line.at(j) += summer;
            
        }
        //Arrange the line numbers in orderly manner
        std::sort(result[i].val.line.begin(),result[i].val.line.end());

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
    FILE* check_list_f;
    check_list_f = fopen("./inverted_index/given_list.txt", "r");

    get_time (begin);

    // Make sure a filename is specified
    if (argv[1] == NULL)
    {
        printf("USAGE: %s <filename> [Top # of results to display]\n", argv[0]);
        exit(1);
    }

    fname = argv[1];
    disp_num_str = argv[2];

    printf("Inverted_index: Running...\n");

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

    //read stop words
    if (!check_list_f) {
        printf("Unable to open file stopwords.txt\n");
        exit(1);   // call system to stop
    }
    
    // Get the number of results to display
    CHECK_ERROR((disp_num = (disp_num_str == NULL) ? 
      DEFAULT_DISP_NUM : atoi(disp_num_str)) <= 0);

    get_time (end);

    #ifdef TIMING
    print_time("initialize", begin, end);
    #endif

    printf("Inverted_index: Calling MapReduce Scheduler Wordcount\n");
    get_time (begin);
    std::vector<WordsMR::keyval> result;    
    WordsMR mapReduce(fdata, finfo.st_size, 1024*1024);

    //Words to find line numbers
    char stop_word[20];
     while (fscanf(check_list_f,"%15s",stop_word) != EOF )
     {
         char temp[1];
        char *tt = temp;
        for(int i = 0; i < 20; i++)
            if(!isalpha(stop_word[i]))
            {
                tt[i] = 0;
                break;
            }
            else tt[i] = stop_word[i];
            
         mapReduce.addWord(tt);
     }
    close(check_list_f);
    CHECK_ERROR( mapReduce.run(result) < 0);
    get_time (end);

    #ifdef TIMING
    print_time("library", begin, end);
    #endif
    printf("Inverted_index: MapReduce Completed\n");

    get_time (begin);

    printf("\nInverted_index Results:\n");

    //mapReduce.display(disp_num);
    mapReduce.fix_arrange(result);

    for (size_t i = 0; i < result.size(); i++)
    {
        printf("%15s - ", result[i].key.data);

        for (size_t j = 0; j < result[i].val.line.size(); j++){
            if(j>=disp_num)break;
            printf("%lu ", result[i].val.line.at(j));

        }
        
        
        printf("\n\n");
    }

    

    free (fdata);

    CHECK_ERROR(close(fd) < 0);

    get_time (end);

    #ifdef TIMING
    print_time("finalize", begin, end);
    #endif

    return 0;
}

// vim: ts=8 sw=4 sts=4 smarttab smartindent
