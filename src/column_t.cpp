#include <column_t.h>

Column_t::Column_t() : total_size(0) {}

Column_t::Column_t(size_t expected_rows) : total_size(0) {
    size_t pages_needed = (expected_rows + PAGE_T_SIZE - 1) / PAGE_T_SIZE;
    pages.assign(pages_needed, Page_t());
}

void Column_t::push_back(const value_t& v) {
    size_t pageIndex = total_size >> PAGE_SHIFT;
    size_t offset = total_size & PAGE_MASK;

    if (offset == 0 && pages.size() <= pageIndex) {
        // need a new page
        pages.push_back(Page_t());
    }

    pages[pageIndex].values[offset] = v;
    total_size++;
}

value_t Column_t::get_at(size_t index) const {
    if (index >= total_size) return value_t::null_value();

    size_t pageIndex = index >> PAGE_SHIFT;
    size_t offset = index & PAGE_MASK;

    return pages[pageIndex].values[offset];
}