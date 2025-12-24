#include <column_t.h>

ValueColumn::ValueColumn() : total_size(0) {}

ValueColumn::ValueColumn(size_t expected_rows) : total_size(0) {
    size_t pages_needed = (expected_rows + PAGE_T_SIZE - 1) / PAGE_T_SIZE;
    pages.assign(pages_needed, Page_t());
}

void ValueColumn::push_back(const value_t& v) {
    size_t pageIndex = total_size >> PAGE_SHIFT;
    size_t offset = total_size & PAGE_MASK;

    if (offset == 0 && pages.size() <= pageIndex) {
        pages.push_back(Page_t());
    }

    pages[pageIndex].values[offset] = v;
    total_size++;
}

value_t ValueColumn::get_at(size_t index) const {
    if (index >= total_size) {
        value_t v;
        return v.null_value();
    }

    size_t pageIndex = index >> PAGE_SHIFT;
    size_t offset = index & PAGE_MASK;

    return pages[pageIndex].values[offset];
}

size_t ValueColumn::size() const {
    return total_size;
}
size_t ValueColumn::page_num() const{
    return pages.size();
}

PageColumn::PageColumn() : total_size(0){}

PageColumn::PageColumn(size_t expected_rows)
    : total_size(0){}

value_t PageColumn::get_at(size_t index) const {
    value_t v;
    if (index >= total_size)
        return v.null_value();

    size_t pageIndex = index / INT32_ROWS_PER_PAGE;
    size_t offset = index % INT32_ROWS_PER_PAGE;

    return v.from_int32(pages[pageIndex][offset]);
}


void PageColumn::push_page(int32_t* data, uint16_t num_rows) {
    pages.push_back(data);
    total_size += num_rows;
}


size_t PageColumn::size() const {
    return total_size;
}
size_t PageColumn::page_num() const{
    return pages.size();
}


Column_t::Column_t() {
    impl = std::make_unique<ValueColumn>();
}

Column_t::Column_t(ColumnStorage s, size_t expected_rows)
    : storage(s)
{
    switch (storage) {
        case ColumnStorage::ValueOwned:
            impl = std::make_unique<ValueColumn>(expected_rows);
            break;

        case ColumnStorage::PageOwned:
            impl = std::make_unique<PageColumn>(expected_rows);
            break;
    }
}

void Column_t::push_back(const value_t& v) {
    assert(storage == ColumnStorage::ValueOwned);
    static_cast<ValueColumn*>(impl.get())->push_back(v);
}

void Column_t::push_page(int32_t* page , uint16_t num_rows){
    assert(storage == ColumnStorage::PageOwned);
    static_cast<PageColumn*>(impl.get())->push_page(page , num_rows);    
}

value_t Column_t::get_at(size_t i) const {
    return impl->get_at(i);
}

size_t Column_t::size() const {
    return impl->size();
}

size_t Column_t::page_num() const {
    return impl->page_num();
}