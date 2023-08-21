#include "utils/read/ReadeIVF.h"

#include <utility>
#include <algorithm>

static uint32_t fourcc (const char sx[4]) {
    assert(4 == strlen(sx));
    const unsigned char *x = (unsigned char*)sx;
    return x[0] | x[1] << 8 | x[2] << 16 | x[3] << 24;
}

static void read_index_header (faiss::Index *idx, faiss::IOReader *f) {
    READ1 (idx->d);
    READ1 (idx->ntotal);
    faiss::Index::idx_t dummy;
    READ1 (dummy);
    READ1 (dummy);
    READ1 (idx->is_trained);
    READ1 (idx->metric_type);
    if (idx->metric_type > 1) {
        READ1 (idx->metric_arg);
    }
    idx->verbose = false;
}

static void read_direct_map (faiss::DirectMap *dm, faiss::IOReader *f) {
    char maintain_direct_map;
    READ1 (maintain_direct_map);
    dm->type = (faiss::DirectMap::Type)maintain_direct_map;
    READVECTOR (dm->array);
    if (dm->type == faiss::DirectMap::Hashtable) {
        using idx_t = faiss::Index::idx_t;
        std::vector<std::pair<idx_t, idx_t>> v;
        READVECTOR (v);
        std::unordered_map<idx_t, idx_t> & map = dm->hashtable;
        map.reserve (v.size());
        for (auto it: v) {
            map [it.first] = it.second;
        }
    }

}

static void read_ivf_header (
        faiss::IndexIVF *ivf, faiss::IOReader *f,
        std::vector<std::vector<faiss::Index::idx_t> > *ids = nullptr)
{
    read_index_header (ivf, f);
    READ1 (ivf->nlist);
    READ1 (ivf->nprobe);
    ivf->quantizer = read_index (f);
    ivf->own_fields = true;
    if (ids) { // used in legacy "Iv" formats
        ids->resize (ivf->nlist);
        for (size_t i = 0; i < ivf->nlist; i++)
            READVECTOR ((*ids)[i]);
    }
    read_direct_map (&ivf->direct_map, f);
}

static void read_ProductQuantizer (faiss::ProductQuantizer *pq, faiss::IOReader *f) {
    READ1 (pq->d);
    READ1 (pq->M);
    READ1 (pq->nbits);
    pq->set_derived_values ();
    READVECTOR (pq->centroids);
}

static void read_ArrayInvertedLists_sizes (
        faiss::IOReader *f, std::vector<size_t> & sizes)
{
    uint32_t list_type;
    READ1(list_type);
    if (list_type == fourcc("full")) {
        size_t os = sizes.size();
        READVECTOR (sizes);
        FAISS_THROW_IF_NOT (os == sizes.size());
    }
    else {
        FAISS_THROW_MSG ("invalid list_type");
    }
}

faiss::InvertedLists *read_InvertedLists_per_centroid (faiss::IOReader *f, const vector<long>& centroids,
        vector<int> parts, float r, int io_flags) {
    uint32_t h;
    READ1 (h);
    if (h == fourcc ("il00")) {
        fprintf(stderr, "read_InvertedLists:"
                        " WARN! inverted lists not stored with IVF object\n");
        return nullptr;
    } else if (h == fourcc ("ilar") && !(io_flags & faiss::IO_FLAG_MMAP)) {
        auto ails = new faiss::ArrayInvertedLists (0, 0);
        READ1 (ails->nlist);
        READ1 (ails->code_size);
        ails->ids.resize (ails->nlist);
        ails->codes.resize (ails->nlist);
        std::vector<size_t> sizes (ails->nlist);
        read_ArrayInvertedLists_sizes (f, sizes);

        for (long c = 0; c < ails->nlist; c++) {
            if(std::find(centroids.begin(), centroids.end(), c) != centroids.end()) {
                long cent = c;
                std::vector<uint8_t> codes_tmp;
                std::vector<int64_t> ids_tmp;

                //ails->ids[cent].resize(sizes[cent]);
                //ails->codes[cent].resize(sizes[cent] * ails->code_size);
                ids_tmp.resize(sizes[cent]);
                codes_tmp.resize(sizes[cent] * ails->code_size);

                //long n = ails->ids[cent].size();
                long n = ids_tmp.size();


                // Getting slices
                int slices = parts[0];//5;
                int crr_slice = parts[1];//4;

                long slice_n;
                if (n == 0) {
                    slice_n = n;
                } else {
                    slice_n = n / slices;
                }

                size_t start_id = (crr_slice) * slice_n;

                if (crr_slice == (slices - 1)) {
                    slice_n = sizes[cent] - (slice_n * crr_slice);
                }

                size_t end_id = (start_id + slice_n);
                if (end_id > 0) {
                    end_id -= 1;
                }

                //if (cent == 4095) {
                //    cout << "TOTAL: " << sizes[cent] << endl;
                //    cout << "SLICE " << crr_slice << " : " << slice_n << endl;
                //    cout << "START " << start_id << " END: " << end_id << endl;
                //}

                //if (r < 1) {
                //    n = round(float(n) * r);
                //    ails->ids[cent].resize(n);
                //    ails->codes[cent].resize(n * ails->code_size);
               // }

                if (n > 0) {
                    //READANDCHECK (ails->codes[cent].data(), n * ails->code_size);
                    //READANDCHECK (ails->ids[cent].data(), n);

                    READANDCHECK (codes_tmp.data(), n * ails->code_size);
                    READANDCHECK (ids_tmp.data(), n);

                    //ails->codes[cent].insert(ails->codes[cent].end(), ails->codes[cent].begin(), ails->codes[cent].end());
                    //ails->ids[cent].insert(ails->ids[cent].end(), ails->ids[cent].begin(), ails->ids[cent].end());

                    int pos = 0;
                    if (slice_n > 0) {
                        for (int j = start_id; j <= end_id; j++, pos++) {
                            //ails->ids[cent][pos] = ails->ids[cent][j];
                            ids_tmp[pos] = ids_tmp[j];

                            for (int d = 0; d < ails->code_size; d++) {
                                //ails->codes[cent][pos * ails->code_size + d] = ails->codes[cent][j * ails->code_size + d];
                                codes_tmp[pos * ails->code_size + d] = codes_tmp[j * ails->code_size + d];
                            }
                        }
                    }

                    //ails->ids[cent].resize(pos);
                    //ails->ids[cent].shrink_to_fit();
                    //ails->codes[cent].resize(pos * ails->code_size);
                    //ails->codes[cent].shrink_to_fit();

                    ids_tmp.resize(pos);
                    ids_tmp.shrink_to_fit();
                    codes_tmp.resize(pos * ails->code_size);
                    codes_tmp.shrink_to_fit();

                    // Replicate here
                    int inteiro = floor(r);
                    float quebrado = r - inteiro;

                    if (inteiro > 0) {
                        for (int i = 0; i < inteiro; i++) {
                            ails->codes[cent].insert(ails->codes[cent].end(), codes_tmp.begin(), codes_tmp.end());
                            ails->ids[cent].insert(ails->ids[cent].end(), ids_tmp.begin(), ids_tmp.end());
                        }
                    }

                    if (quebrado > 0) {
                        ids_tmp.resize(pos * quebrado);
                        ids_tmp.shrink_to_fit();
                        codes_tmp.resize(pos * quebrado * ails->code_size);
                        codes_tmp.shrink_to_fit();

                        ails->codes[cent].insert(ails->codes[cent].end(), codes_tmp.begin(), codes_tmp.end());
                        ails->ids[cent].insert(ails->ids[cent].end(), ids_tmp.begin(), ids_tmp.end());
                    }

                    //if (r < 1) {
                        // Downsampling
                    //    ails->ids[cent].resize(pos * r);
                    //    ails->ids[cent].shrink_to_fit();
                    //    ails->codes[cent].resize(pos * r * ails->code_size);
                    //    ails->codes[cent].shrink_to_fit();
                    //} 

                    //if (r > 1) {
                        // Oversampling Exponential
                    //    for (int i = 1; i < r; i++) {
                    //        ails->codes[cent].insert(ails->codes[cent].end(), ails->codes[cent].begin(),
                    //                             ails->codes[cent].end());
                    //        ails->ids[cent].insert(ails->ids[cent].end(), ails->ids[cent].begin(), ails->ids[cent].end());
                    //    }
                    //}

                    //if (cent == 4095) {
                    //    cout << pos << endl;
                    //    cout << ails->ids[cent].size() << " " <<  endl;
                    //    for (int ccc = 0; ccc < ails->ids[cent].size(); ccc++) {
                    //        cout << ails->ids[cent][ccc] << " ";
                    //    }
                    //    cout << endl;
                    //}
                }
            } else {
                long n = sizes[c];
                vector<long> trashIds;
                trashIds.resize(n);
                vector<uint8_t> trashCodes;
                trashCodes.resize( n * ails->code_size);
                READANDCHECK (trashCodes.data(), n * ails->code_size);
                READANDCHECK (trashIds.data(), n);
                std::vector<long>().swap(trashIds);
                std::vector<uint8_t>().swap(trashCodes);
            }
        }
        return ails;
    } else {
        FAISS_THROW_MSG ("read_InvertedLists: unsupported invlist type");
    }
}

faiss::InvertedLists *read_InvertedLists_equally(faiss::IOReader *f, float start_percent, float end_percent, float r,
        int io_flags) {
    uint32_t h;
    READ1 (h);
    if (h == fourcc ("il00")) {
        fprintf(stderr, "read_InvertedLists:"
                        " WARN! inverted lists not stored with IVF object\n");
        return nullptr;
    } else if (h == fourcc ("ilar") && !(io_flags & faiss::IO_FLAG_MMAP)) {
        auto ails = new faiss::ArrayInvertedLists (0, 0);
        READ1 (ails->nlist);
        READ1 (ails->code_size);
        ails->ids.resize (ails->nlist);
        ails->codes.resize (ails->nlist);
        std::vector<size_t> sizes (ails->nlist);
        read_ArrayInvertedLists_sizes (f, sizes);

        for (size_t i = 0; i < ails->nlist; i++) {
            ails->ids[i].resize(sizes[i]);
            ails->codes[i].resize(sizes[i] * ails->code_size);

            size_t n = ails->ids[i].size();
            if (r < 1) {
                n = round (float(n) * r);
                ails->ids[i].resize(n);
                ails->codes[i].resize(n * ails->code_size);
            }

            size_t start_id = std::ceil(n * start_percent);
            size_t end_id = std::ceil(n * end_percent) - 1;

            if (n > 0) {
                READANDCHECK(ails->codes[i].data(), n * ails->code_size);
                READANDCHECK(ails->ids[i].data(), n);

                // here we throw away the entries that are not IN this shard
                int c = 0;
                for (int j = start_id; j <= end_id; j++, c++) {
                    ails->ids[i][c] = ails->ids[i][j];

                    for (int d = 0; d < ails->code_size; d++) {
                        ails->codes[i][c * ails->code_size + d] = ails->codes[i][j * ails->code_size + d];
                    }
                }

                int size = ails->ids[i].size();
                ails->ids[i].resize(c);
                ails->ids[i].shrink_to_fit();
                ails->codes[i].resize(c * ails->code_size);
                ails->codes[i].shrink_to_fit();

                // replication
                for (int rep = 1; rep < r; rep++) {
                    ails->codes[i].insert(ails->codes[i].end(), ails->codes[i].begin(), ails->codes[i].end());
                    ails->ids[i].insert(ails->ids[i].end(), ails->ids[i].begin(), ails->ids[i].end());
                }

              // *ntotal += ails->ids.size();
            }
        }

        return ails;
    } else {
        FAISS_THROW_MSG ("read_InvertedLists: unsupported invlist type");
    }
}

static void read_InvertedLists (faiss::IndexIVF *ivf, faiss::IOReader *f, const vector<long>& centroids, vector<int> parts,
        float r, int io_flags) {

    faiss::InvertedLists *ils;
    if (centroids[0] == -1) {
        // Equally distribution
        float start_perc = (float)centroids[1] / 100.0;
        float end_perc = (float)centroids[2] / 100.0;
        ils = read_InvertedLists_equally (f, start_perc, end_perc, r, io_flags);

    } else {
        ils = read_InvertedLists_per_centroid (f, centroids, parts, r, io_flags);
    }

    FAISS_THROW_IF_NOT (!ils || (ils->nlist == ivf->nlist &&
                                 ils->code_size == ivf->code_size));
    ivf->invlists = ils;
    ivf->own_invlists = true;
}

static faiss::ArrayInvertedLists *set_array_invlist(
       faiss::IndexIVF *ivf, std::vector<std::vector<faiss::Index::idx_t> > &ids)
{
    auto *ail = new faiss::ArrayInvertedLists (
            ivf->nlist, ivf->code_size);
    std::swap (ail->ids, ids);
    ivf->invlists = ail;
    ivf->own_invlists = true;
    return ail;
}

static faiss::IndexIVFPQ *read_ivfpq (faiss::IOReader *f, uint32_t h, const vector<long>& centroids, vector<int> parts,
        float r, int io_flags)
{
    bool legacy = h == fourcc ("IvQR") || h == fourcc ("IvPQ");

    faiss::IndexIVFPQR *ivfpqr =
            h == fourcc ("IvQR") || h == fourcc ("IwQR") ?
            new faiss::IndexIVFPQR () : nullptr;

    faiss::IndexIVFPQ * ivpq = ivfpqr ? ivfpqr : new faiss::IndexIVFPQ();

    std::vector<std::vector<faiss::Index::idx_t> > ids;
    read_ivf_header (ivpq, f, legacy ? &ids : nullptr);
    READ1 (ivpq->by_residual);
    READ1 (ivpq->code_size);
    read_ProductQuantizer (&ivpq->pq, f);

    if (legacy) {
        faiss::ArrayInvertedLists *ail = set_array_invlist (ivpq, ids);
        for (size_t i = 0; i < ail->nlist; i++)
            READVECTOR (ail->codes[i]);
    } else {
        read_InvertedLists (ivpq, f, std::move(centroids), std::move(parts), r, io_flags);
    }

    if (ivpq->is_trained) {
        // precomputed table not stored. It is cheaper to recompute it
        ivpq->use_precomputed_table = 0;
        if (ivpq->by_residual)
            ivpq->precompute_table ();
        if (ivfpqr) {
            read_ProductQuantizer (&ivfpqr->refine_pq, f);
            READVECTOR (ivfpqr->refine_codes);
            READ1 (ivfpqr->k_factor);
        }
    }
    return ivpq;
}

faiss::Index *read_index_per_centroid(faiss::IOReader *f, vector<long> centroids, vector<int> parts, float r, int io_flags) {
    faiss::Index * idx = nullptr;
    uint32_t h;
    READ1 (h);
    if(h == fourcc ("IvPQ") || h == fourcc ("IvQR") ||
       h == fourcc ("IwPQ") || h == fourcc ("IwQR")) {
        idx = read_ivfpq(f, h, std::move(centroids), std::move(parts), r, io_flags);
    }
    return idx;
}

faiss::Index *read_index_especial (const char *fname, vector<long> centroids, vector<int> parts, float r, int io_flags) {
    faiss::FileIOReader reader(fname);
    faiss::Index *idx = read_index_per_centroid(&reader, std::move(centroids), std::move(parts), r, io_flags);
    return idx;
}
