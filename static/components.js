// Reusable UI Components for Market Sizing Tool

// ============================================================
// CheckboxMultiSelect — searchable checkbox dropdown with tags
// ============================================================
class CheckboxMultiSelect {
    constructor(container, options, { placeholder = 'Select...', selected = [], searchable = true, onChange = null } = {}) {
        this.container = typeof container === 'string' ? document.querySelector(container) : container;
        this.options = options; // array of strings
        this.selected = new Set(selected);
        this.searchable = searchable;
        this.onChange = onChange;
        this.isOpen = false;
        this.render();
    }

    render() {
        this.container.innerHTML = '';
        this.container.classList.add('relative');

        // Trigger button
        this.trigger = document.createElement('div');
        this.trigger.className = 'border rounded-md p-2 cursor-pointer bg-white min-h-[38px] flex flex-wrap gap-1 items-center text-sm';
        this.trigger.addEventListener('click', (e) => {
            if (e.target.closest('.ms-tag-remove')) return;
            this.toggle();
        });
        this.container.appendChild(this.trigger);

        // Dropdown
        this.dropdown = document.createElement('div');
        this.dropdown.className = 'absolute z-50 mt-1 w-full bg-white border rounded-md shadow-lg max-h-60 overflow-auto hidden';
        this.dropdown.style.minWidth = '200px';

        if (this.searchable) {
            const searchWrap = document.createElement('div');
            searchWrap.className = 'p-2 border-b sticky top-0 bg-white';
            this.searchInput = document.createElement('input');
            this.searchInput.type = 'text';
            this.searchInput.placeholder = 'Search...';
            this.searchInput.className = 'w-full border rounded px-2 py-1 text-sm outline-none focus:border-blue-400';
            this.searchInput.addEventListener('input', () => this.filterOptions());
            this.searchInput.addEventListener('click', (e) => e.stopPropagation());
            searchWrap.appendChild(this.searchInput);
            this.dropdown.appendChild(searchWrap);
        }

        this.optionsList = document.createElement('div');
        this.optionsList.className = 'p-1';
        this.dropdown.appendChild(this.optionsList);
        this.container.appendChild(this.dropdown);

        this.renderOptions();
        this.updateTrigger();

        // Close on outside click
        document.addEventListener('click', (e) => {
            if (!this.container.contains(e.target)) this.close();
        });
    }

    renderOptions(filter = '') {
        this.optionsList.innerHTML = '';
        const lf = filter.toLowerCase();
        this.options.forEach(opt => {
            if (lf && !opt.toLowerCase().includes(lf)) return;
            const label = document.createElement('label');
            label.className = 'flex items-center gap-2 px-2 py-1.5 hover:bg-gray-50 rounded cursor-pointer text-sm';
            const cb = document.createElement('input');
            cb.type = 'checkbox';
            cb.checked = this.selected.has(opt);
            cb.className = 'rounded';
            cb.addEventListener('change', (e) => {
                e.stopPropagation(); // Prevent event bubbling
                if (cb.checked) this.selected.add(opt);
                else this.selected.delete(opt);
                this.updateTrigger();
                if (this.onChange) this.onChange(this.getSelected());
                // Don't close dropdown - keep it open for multiple selections
            });
            label.appendChild(cb);
            label.appendChild(document.createTextNode(opt));
            this.optionsList.appendChild(label);
        });
    }

    filterOptions() {
        this.renderOptions(this.searchInput ? this.searchInput.value : '');
    }

    updateTrigger() {
        this.trigger.innerHTML = '';
        if (this.selected.size === 0) {
            const ph = document.createElement('span');
            ph.className = 'text-gray-400';
            ph.textContent = 'Select...';
            this.trigger.appendChild(ph);
            return;
        }
        this.selected.forEach(val => {
            const tag = document.createElement('span');
            tag.className = 'inline-flex items-center gap-1 bg-blue-100 text-blue-800 text-xs px-2 py-0.5 rounded';
            tag.innerHTML = `${this.escHtml(val)}<button type="button" class="ms-tag-remove hover:text-blue-600">&times;</button>`;
            tag.querySelector('.ms-tag-remove').addEventListener('click', (e) => {
                e.stopPropagation();
                this.selected.delete(val);
                this.updateTrigger();
                this.renderOptions(this.searchInput ? this.searchInput.value : '');
                if (this.onChange) this.onChange(this.getSelected());
            });
            this.trigger.appendChild(tag);
        });
    }

    toggle() { this.isOpen ? this.close() : this.open(); }
    open() {
        this.dropdown.classList.remove('hidden');
        this.isOpen = true;
        if (this.searchInput) { this.searchInput.value = ''; this.filterOptions(); this.searchInput.focus(); }
    }
    close() { this.dropdown.classList.add('hidden'); this.isOpen = false; }
    getSelected() { return Array.from(this.selected); }
    setSelected(arr) { this.selected = new Set(arr); this.updateTrigger(); this.renderOptions(); }
    escHtml(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }
}

// ============================================================
// IncludeExcludeSelect — tabs for include/exclude with nested dept tree or flat list
// ============================================================
class IncludeExcludeSelect {
    constructor(container, options, { placeholder = 'Select...', tree = false, searchable = true, includeSelected = [], excludeSelected = [], onChange = null } = {}) {
        this.container = typeof container === 'string' ? document.querySelector(container) : container;
        this.options = options;
        this.tree = tree;
        this.searchable = searchable;
        this.includeSet = new Set(includeSelected);
        this.excludeSet = new Set(excludeSelected);
        this.activeTab = 'include';
        this.onChange = onChange;
        this.render();
    }

    render() {
        this.container.innerHTML = '';
        this.container.classList.add('relative');

        // Tabs
        const tabs = document.createElement('div');
        tabs.className = 'flex mb-1';
        ['include', 'exclude'].forEach(tab => {
            const btn = document.createElement('button');
            btn.type = 'button';
            btn.textContent = tab.charAt(0).toUpperCase() + tab.slice(1);
            btn.className = `px-3 py-1 text-xs font-medium rounded-t border-b-2 ${this.activeTab === tab ? 'border-blue-500 text-blue-600 bg-blue-50' : 'border-transparent text-gray-500 hover:text-gray-700'}`;
            btn.addEventListener('click', () => { this.activeTab = tab; this.render(); });
            tabs.appendChild(btn);
        });
        this.container.appendChild(tabs);

        // Tags display
        const tagsDiv = document.createElement('div');
        tagsDiv.className = 'flex flex-wrap gap-1 mb-1 min-h-[24px]';
        const currentSet = this.activeTab === 'include' ? this.includeSet : this.excludeSet;
        const tagColor = this.activeTab === 'include' ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800';
        currentSet.forEach(val => {
            const tag = document.createElement('span');
            tag.className = `inline-flex items-center gap-1 ${tagColor} text-xs px-2 py-0.5 rounded`;
            tag.innerHTML = `${this.escHtml(val)}<button type="button" class="hover:opacity-70">&times;</button>`;
            tag.querySelector('button').addEventListener('click', () => {
                currentSet.delete(val);
                this.render();
                if (this.onChange) this.onChange(this.getValues());
            });
            tagsDiv.appendChild(tag);
        });
        this.container.appendChild(tagsDiv);

        // Dropdown trigger
        const wrapper = document.createElement('div');
        wrapper.className = 'relative';

        const trigger = document.createElement('div');
        trigger.className = 'border rounded-md p-1.5 cursor-pointer bg-white text-sm text-gray-400';
        trigger.textContent = `+ Add to ${this.activeTab}...`;
        trigger.addEventListener('click', () => this.toggleDropdown());
        wrapper.appendChild(trigger);

        // Dropdown panel
        this.dropdownEl = document.createElement('div');
        this.dropdownEl.className = 'absolute z-50 mt-1 w-full bg-white border rounded-md shadow-lg max-h-48 overflow-auto hidden';
        this.dropdownEl.style.minWidth = '200px';

        if (this.searchable) {
            const sw = document.createElement('div');
            sw.className = 'p-2 border-b sticky top-0 bg-white';
            this.sInput = document.createElement('input');
            this.sInput.type = 'text';
            this.sInput.placeholder = 'Search...';
            this.sInput.className = 'w-full border rounded px-2 py-1 text-sm outline-none focus:border-blue-400';
            this.sInput.addEventListener('input', () => this.renderDropdownItems());
            this.sInput.addEventListener('click', (e) => e.stopPropagation());
            sw.appendChild(this.sInput);
            this.dropdownEl.appendChild(sw);
        }

        this.itemsEl = document.createElement('div');
        this.itemsEl.className = 'p-1';
        this.dropdownEl.appendChild(this.itemsEl);
        wrapper.appendChild(this.dropdownEl);
        this.container.appendChild(wrapper);

        this.renderDropdownItems();

        document.addEventListener('click', (e) => {
            if (!this.container.contains(e.target)) this.closeDropdown();
        });
    }

    renderDropdownItems() {
        this.itemsEl.innerHTML = '';
        const filter = (this.sInput ? this.sInput.value : '').toLowerCase();
        const currentSet = this.activeTab === 'include' ? this.includeSet : this.excludeSet;

        if (this.tree) {
            this.options.forEach(group => {
                if (filter && !group.name.toLowerCase().includes(filter) && !group.children.some(c => c.toLowerCase().includes(filter))) return;
                const groupEl = document.createElement('div');
                groupEl.className = 'mb-1';

                const parentLabel = document.createElement('label');
                parentLabel.className = 'flex items-center gap-2 px-2 py-1 hover:bg-gray-50 rounded cursor-pointer text-sm font-medium';
                const pcb = document.createElement('input');
                pcb.type = 'checkbox';
                pcb.checked = currentSet.has(group.name);
                pcb.className = 'rounded';
                pcb.addEventListener('change', (e) => {
                    e.stopPropagation(); // Prevent event bubbling
                    if (pcb.checked) {
                        currentSet.add(group.name);
                    } else {
                        currentSet.delete(group.name);
                    }
                    this.updateTagsDisplay();
                    if (this.onChange) this.onChange(this.getValues());
                });
                parentLabel.appendChild(pcb);

                const arrow = document.createElement('span');
                arrow.className = 'cursor-pointer text-gray-400 text-xs select-none';
                arrow.textContent = '▶';
                arrow.addEventListener('click', (e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    const childrenEl = groupEl.querySelector('.dept-children');
                    if (childrenEl) {
                        childrenEl.classList.toggle('hidden');
                        arrow.textContent = childrenEl.classList.contains('hidden') ? '▶' : '▼';
                    }
                });
                parentLabel.appendChild(arrow);
                parentLabel.appendChild(document.createTextNode(group.name));
                groupEl.appendChild(parentLabel);

                const childrenEl = document.createElement('div');
                childrenEl.className = 'dept-children ml-6 hidden';
                group.children.forEach(child => {
                    if (filter && !child.toLowerCase().includes(filter) && !group.name.toLowerCase().includes(filter)) return;
                    const cl = document.createElement('label');
                    cl.className = 'flex items-center gap-2 px-2 py-1 hover:bg-gray-50 rounded cursor-pointer text-sm';
                    const ccb = document.createElement('input');
                    ccb.type = 'checkbox';
                    ccb.checked = currentSet.has(child);
                    ccb.className = 'rounded';
                    ccb.addEventListener('change', (e) => {
                        e.stopPropagation(); // Prevent event bubbling
                        if (ccb.checked) currentSet.add(child);
                        else currentSet.delete(child);
                        this.updateTagsDisplay();
                        if (this.onChange) this.onChange(this.getValues());
                    });
                    cl.appendChild(ccb);
                    cl.appendChild(document.createTextNode(child));
                    childrenEl.appendChild(cl);
                });

                if (filter) childrenEl.classList.remove('hidden');
                groupEl.appendChild(childrenEl);
                this.itemsEl.appendChild(groupEl);
            });
        } else {
            this.options.forEach(opt => {
                if (filter && !opt.toLowerCase().includes(filter)) return;
                const label = document.createElement('label');
                label.className = 'flex items-center gap-2 px-2 py-1.5 hover:bg-gray-50 rounded cursor-pointer text-sm';
                const cb = document.createElement('input');
                cb.type = 'checkbox';
                cb.checked = currentSet.has(opt);
                cb.className = 'rounded';
                cb.addEventListener('change', (e) => {
                    e.stopPropagation(); // Prevent event bubbling
                    if (cb.checked) currentSet.add(opt);
                    else currentSet.delete(opt);
                    this.updateTagsDisplay();
                    if (this.onChange) this.onChange(this.getValues());
                });
                label.appendChild(cb);
                label.appendChild(document.createTextNode(opt));
                this.itemsEl.appendChild(label);
            });
        }
    }

    updateTagsDisplay() {
        // Update only the tags display without re-rendering the entire component
        const tagsDiv = this.container.querySelector('.flex.flex-wrap.gap-1.mb-1.min-h-\\[24px\\]');
        if (tagsDiv) {
            tagsDiv.innerHTML = '';
            const currentSet = this.activeTab === 'include' ? this.includeSet : this.excludeSet;
            const tagColor = this.activeTab === 'include' ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800';
            currentSet.forEach(val => {
                const tag = document.createElement('span');
                tag.className = `inline-flex items-center gap-1 ${tagColor} text-xs px-2 py-0.5 rounded`;
                tag.innerHTML = `${this.escHtml(val)}<button type="button" class="hover:opacity-70">&times;</button>`;
                tag.querySelector('button').addEventListener('click', () => {
                    currentSet.delete(val);
                    this.updateTagsDisplay();
                    if (this.onChange) this.onChange(this.getValues());
                });
                tagsDiv.appendChild(tag);
            });
        }
    }

    toggleDropdown() {
        this.dropdownEl.classList.toggle('hidden');
        if (!this.dropdownEl.classList.contains('hidden') && this.sInput) {
            this.sInput.value = '';
            this.renderDropdownItems();
            this.sInput.focus();
        }
    }
    closeDropdown() { if (this.dropdownEl) this.dropdownEl.classList.add('hidden'); }
    getValues() { return { include: Array.from(this.includeSet), exclude: Array.from(this.excludeSet) }; }
    escHtml(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }
}

// ============================================================
// LocationAutocomplete — type-ahead for Prospeo Search Suggestions API
// ============================================================
class LocationAutocomplete {
    constructor(container, { includeValues = [], excludeValues = [], onChange = null } = {}) {
        this.container = typeof container === 'string' ? document.querySelector(container) : container;
        this.includeValues = [...includeValues];
        this.excludeValues = [...excludeValues];
        this.activeTab = 'include';
        this.onChange = onChange;
        this.debounceTimer = null;
        this.render();
    }

    render() {
        this.container.innerHTML = '';

        // Tabs
        const tabs = document.createElement('div');
        tabs.className = 'flex mb-1';
        ['include', 'exclude'].forEach(tab => {
            const btn = document.createElement('button');
            btn.type = 'button';
            btn.textContent = tab.charAt(0).toUpperCase() + tab.slice(1);
            btn.className = `px-3 py-1 text-xs font-medium rounded-t border-b-2 ${this.activeTab === tab ? 'border-blue-500 text-blue-600 bg-blue-50' : 'border-transparent text-gray-500 hover:text-gray-700'}`;
            btn.addEventListener('click', () => { this.activeTab = tab; this.render(); });
            tabs.appendChild(btn);
        });
        this.container.appendChild(tabs);

        // Tags
        const tagsDiv = document.createElement('div');
        tagsDiv.className = 'flex flex-wrap gap-1 mb-1 min-h-[24px]';
        const currentArr = this.activeTab === 'include' ? this.includeValues : this.excludeValues;
        const tagColor = this.activeTab === 'include' ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800';
        currentArr.forEach((val, i) => {
            const tag = document.createElement('span');
            tag.className = `inline-flex items-center gap-1 ${tagColor} text-xs px-2 py-0.5 rounded`;
            tag.innerHTML = `${this.escHtml(val)}<button type="button" class="hover:opacity-70">&times;</button>`;
            tag.querySelector('button').addEventListener('click', () => {
                currentArr.splice(i, 1);
                this.render();
                if (this.onChange) this.onChange(this.getValues());
            });
            tagsDiv.appendChild(tag);
        });
        this.container.appendChild(tagsDiv);

        // Input + dropdown
        const wrapper = document.createElement('div');
        wrapper.className = 'relative';
        this.input = document.createElement('input');
        this.input.type = 'text';
        this.input.placeholder = 'Type to search locations...';
        this.input.className = 'w-full border rounded-md p-1.5 text-sm outline-none focus:border-blue-400';
        this.input.addEventListener('input', () => this.onInputChange());
        wrapper.appendChild(this.input);

        this.suggestionsEl = document.createElement('div');
        this.suggestionsEl.className = 'absolute z-50 mt-1 w-full bg-white border rounded-md shadow-lg max-h-48 overflow-auto hidden';
        wrapper.appendChild(this.suggestionsEl);
        this.container.appendChild(wrapper);

        document.addEventListener('click', (e) => {
            if (!this.container.contains(e.target)) this.suggestionsEl.classList.add('hidden');
        });
    }

    onInputChange() {
        clearTimeout(this.debounceTimer);
        const q = this.input.value.trim();
        if (q.length < 2) { this.suggestionsEl.classList.add('hidden'); return; }
        this.debounceTimer = setTimeout(() => this.fetchSuggestions(q), 300);
    }

    async fetchSuggestions(query) {
        try {
            const resp = await fetch('/api/suggestions', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ location: query })
            });
            const data = await resp.json();
            if (data.error || !data.location_suggestions) return;
            this.showSuggestions(data.location_suggestions);
        } catch (e) { /* ignore */ }
    }

    showSuggestions(suggestions) {
        this.suggestionsEl.innerHTML = '';
        if (!suggestions.length) { this.suggestionsEl.classList.add('hidden'); return; }
        suggestions.forEach(s => {
            const item = document.createElement('div');
            item.className = 'px-3 py-2 hover:bg-gray-50 cursor-pointer text-sm flex justify-between items-center';
            item.innerHTML = `<span>${this.escHtml(s.name)}</span><span class="text-xs text-gray-400">${s.type}</span>`;
            item.addEventListener('click', () => {
                const arr = this.activeTab === 'include' ? this.includeValues : this.excludeValues;
                if (!arr.includes(s.name)) arr.push(s.name);
                this.input.value = '';
                this.suggestionsEl.classList.add('hidden');
                this.render();
                if (this.onChange) this.onChange(this.getValues());
            });
            this.suggestionsEl.appendChild(item);
        });
        this.suggestionsEl.classList.remove('hidden');
    }

    getValues() { return { include: [...this.includeValues], exclude: [...this.excludeValues] }; }
    escHtml(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }
}

// ============================================================
// JobTitleWidget — standard (autocomplete + include/exclude) OR boolean search
// ============================================================
class JobTitleWidget {
    constructor(container, { onChange = null } = {}) {
        this.container = typeof container === 'string' ? document.querySelector(container) : container;
        this.mode = 'standard'; // 'standard' or 'boolean'
        this.includeValues = [];
        this.excludeValues = [];
        this.matchExact = false;
        this.booleanExpr = '';
        this.activeTab = 'include';
        this.onChange = onChange;
        this.debounceTimer = null;
        this.render();
    }

    render() {
        this.container.innerHTML = '';

        // Mode toggle
        const modeDiv = document.createElement('div');
        modeDiv.className = 'flex items-center gap-2 mb-2';
        const modeLabel = document.createElement('span');
        modeLabel.className = 'text-xs text-gray-500';
        modeLabel.textContent = 'Mode:';
        modeDiv.appendChild(modeLabel);

        ['standard', 'boolean'].forEach(m => {
            const btn = document.createElement('button');
            btn.type = 'button';
            btn.textContent = m === 'standard' ? 'Standard' : 'Boolean';
            btn.className = `px-2 py-0.5 text-xs rounded ${this.mode === m ? 'bg-blue-100 text-blue-700 font-medium' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'}`;
            btn.addEventListener('click', () => { this.mode = m; this.render(); });
            modeDiv.appendChild(btn);
        });
        this.container.appendChild(modeDiv);

        if (this.mode === 'boolean') {
            this.renderBooleanMode();
        } else {
            this.renderStandardMode();
        }
    }

    renderBooleanMode() {
        const helpText = document.createElement('p');
        helpText.className = 'text-xs text-gray-500 mb-1';
        helpText.innerHTML = 'Syntax: <code>AND</code> <code>OR</code> <code>()</code> <code>!</code>exclude <code>"exact"</code> <code>\'contains spaces\'</code>';
        this.container.appendChild(helpText);

        const textarea = document.createElement('textarea');
        textarea.className = 'w-full border rounded-md p-2 text-sm outline-none focus:border-blue-400 h-20';
        textarea.placeholder = 'e.g., (SDR OR BDR) AND !intern';
        textarea.value = this.booleanExpr;
        textarea.addEventListener('input', () => {
            this.booleanExpr = textarea.value;
            if (this.onChange) this.onChange(this.getValues());
        });
        this.container.appendChild(textarea);
    }

    renderStandardMode() {
        // Include/Exclude tabs
        const tabs = document.createElement('div');
        tabs.className = 'flex mb-1';
        ['include', 'exclude'].forEach(tab => {
            const btn = document.createElement('button');
            btn.type = 'button';
            btn.textContent = tab.charAt(0).toUpperCase() + tab.slice(1);
            btn.className = `px-3 py-1 text-xs font-medium rounded-t border-b-2 ${this.activeTab === tab ? 'border-blue-500 text-blue-600 bg-blue-50' : 'border-transparent text-gray-500 hover:text-gray-700'}`;
            btn.addEventListener('click', () => { this.activeTab = tab; this.render(); });
            tabs.appendChild(btn);
        });
        this.container.appendChild(tabs);

        // Tags
        const tagsDiv = document.createElement('div');
        tagsDiv.className = 'flex flex-wrap gap-1 mb-1 min-h-[24px]';
        const currentArr = this.activeTab === 'include' ? this.includeValues : this.excludeValues;
        const tagColor = this.activeTab === 'include' ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800';
        currentArr.forEach((val, i) => {
            const tag = document.createElement('span');
            tag.className = `inline-flex items-center gap-1 ${tagColor} text-xs px-2 py-0.5 rounded`;
            tag.innerHTML = `${this.escHtml(val)}<button type="button">&times;</button>`;
            tag.querySelector('button').addEventListener('click', () => {
                currentArr.splice(i, 1);
                this.render();
                if (this.onChange) this.onChange(this.getValues());
            });
            tagsDiv.appendChild(tag);
        });
        this.container.appendChild(tagsDiv);

        // Autocomplete input
        const wrapper = document.createElement('div');
        wrapper.className = 'relative';
        this.input = document.createElement('input');
        this.input.type = 'text';
        this.input.placeholder = `Type job title to ${this.activeTab}...`;
        this.input.className = 'w-full border rounded-md p-1.5 text-sm outline-none focus:border-blue-400';
        this.input.addEventListener('input', () => this.onInput());
        this.input.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' && this.input.value.trim()) {
                e.preventDefault();
                this.addValue(this.input.value.trim());
            }
        });
        wrapper.appendChild(this.input);

        this.suggestionsEl = document.createElement('div');
        this.suggestionsEl.className = 'absolute z-50 mt-1 w-full bg-white border rounded-md shadow-lg max-h-48 overflow-auto hidden';
        wrapper.appendChild(this.suggestionsEl);
        this.container.appendChild(wrapper);

        // Match exact toggle
        const exactDiv = document.createElement('label');
        exactDiv.className = 'flex items-center gap-2 mt-2 text-xs text-gray-600 cursor-pointer';
        const exactCb = document.createElement('input');
        exactCb.type = 'checkbox';
        exactCb.checked = this.matchExact;
        exactCb.addEventListener('change', () => {
            this.matchExact = exactCb.checked;
            if (this.onChange) this.onChange(this.getValues());
        });
        exactDiv.appendChild(exactCb);
        exactDiv.appendChild(document.createTextNode('Match only exact job titles'));
        this.container.appendChild(exactDiv);

        document.addEventListener('click', (e) => {
            if (!this.container.contains(e.target) && this.suggestionsEl) this.suggestionsEl.classList.add('hidden');
        });
    }

    addValue(val) {
        const arr = this.activeTab === 'include' ? this.includeValues : this.excludeValues;
        if (!arr.includes(val)) arr.push(val);
        this.input.value = '';
        this.suggestionsEl.classList.add('hidden');
        this.render();
        if (this.onChange) this.onChange(this.getValues());
    }

    onInput() {
        clearTimeout(this.debounceTimer);
        const q = this.input.value.trim();
        if (q.length < 2) { this.suggestionsEl.classList.add('hidden'); return; }
        this.debounceTimer = setTimeout(() => this.fetchSuggestions(q), 300);
    }

    async fetchSuggestions(query) {
        try {
            const resp = await fetch('/api/suggestions', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ job_title: query })
            });
            const data = await resp.json();
            if (data.error || !data.job_title_suggestions) return;
            this.showSuggestions(data.job_title_suggestions);
        } catch (e) { /* ignore */ }
    }

    showSuggestions(suggestions) {
        this.suggestionsEl.innerHTML = '';
        if (!suggestions.length) { this.suggestionsEl.classList.add('hidden'); return; }
        suggestions.forEach(title => {
            const item = document.createElement('div');
            item.className = 'px-3 py-2 hover:bg-gray-50 cursor-pointer text-sm';
            item.textContent = title;
            item.addEventListener('click', () => this.addValue(title));
            this.suggestionsEl.appendChild(item);
        });
        this.suggestionsEl.classList.remove('hidden');
    }

    getValues() {
        if (this.mode === 'boolean') {
            return { mode: 'boolean', boolean_search: this.booleanExpr };
        }
        return {
            mode: 'standard',
            include: [...this.includeValues],
            exclude: [...this.excludeValues],
            match_only_exact_job_titles: this.matchExact
        };
    }
    escHtml(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }
}

// ============================================================
// HeadcountByDeptWidget — dynamic department + min/max rules
// ============================================================
class HeadcountByDeptWidget {
    constructor(container, { rules = [], onChange = null } = {}) {
        this.container = typeof container === 'string' ? document.querySelector(container) : container;
        this.rules = rules.length ? rules : [];
        this.onChange = onChange;
        this.render();
    }

    render() {
        this.container.innerHTML = '';

        this.rules.forEach((rule, i) => {
            const row = document.createElement('div');
            row.className = 'flex items-center gap-2 mb-2 p-2 bg-gray-50 rounded text-sm';

            // Department selector
            const deptSelect = document.createElement('select');
            deptSelect.className = 'border rounded px-2 py-1 text-sm flex-1';
            deptSelect.innerHTML = '<option value="">Select department</option>';
            DEPARTMENTS.forEach(group => {
                const optgroup = document.createElement('optgroup');
                optgroup.label = group.name;
                const parentOpt = document.createElement('option');
                parentOpt.value = group.name;
                parentOpt.textContent = group.name;
                if (rule.department === group.name) parentOpt.selected = true;
                optgroup.appendChild(parentOpt);
                group.children.forEach(child => {
                    const opt = document.createElement('option');
                    opt.value = child;
                    opt.textContent = '  ' + child;
                    if (rule.department === child) opt.selected = true;
                    optgroup.appendChild(opt);
                });
                deptSelect.appendChild(optgroup);
            });
            deptSelect.addEventListener('change', () => {
                rule.department = deptSelect.value;
                if (this.onChange) this.onChange(this.getRules());
            });
            row.appendChild(deptSelect);

            // Min
            const minInput = document.createElement('input');
            minInput.type = 'number';
            minInput.placeholder = 'Min';
            minInput.className = 'border rounded px-2 py-1 w-16 text-sm';
            minInput.value = rule.min || '';
            minInput.addEventListener('input', () => {
                rule.min = parseInt(minInput.value) || null;
                if (this.onChange) this.onChange(this.getRules());
            });
            row.appendChild(document.createTextNode('Min'));
            row.appendChild(minInput);

            // Max
            const maxInput = document.createElement('input');
            maxInput.type = 'number';
            maxInput.placeholder = 'Max';
            maxInput.className = 'border rounded px-2 py-1 w-16 text-sm';
            maxInput.value = rule.max || '';
            maxInput.addEventListener('input', () => {
                rule.max = parseInt(maxInput.value) || null;
                if (this.onChange) this.onChange(this.getRules());
            });
            row.appendChild(document.createTextNode('Max'));
            row.appendChild(maxInput);

            // Remove
            const removeBtn = document.createElement('button');
            removeBtn.type = 'button';
            removeBtn.className = 'text-red-500 hover:text-red-700 text-lg';
            removeBtn.textContent = '×';
            removeBtn.addEventListener('click', () => {
                this.rules.splice(i, 1);
                this.render();
                if (this.onChange) this.onChange(this.getRules());
            });
            row.appendChild(removeBtn);

            this.container.appendChild(row);
        });

        // Add button
        if (this.rules.length < 10) {
            const addBtn = document.createElement('button');
            addBtn.type = 'button';
            addBtn.className = 'text-blue-600 hover:text-blue-800 text-sm flex items-center gap-1';
            addBtn.textContent = '+ Add department rule';
            addBtn.addEventListener('click', () => {
                this.rules.push({ department: '', min: null, max: null });
                this.render();
            });
            this.container.appendChild(addBtn);
        }
    }

    getRules() {
        return this.rules.filter(r => r.department).map(r => ({
            department: r.department,
            min: r.min || 1,
            max: r.max || 100000
        }));
    }
}

// ============================================================
// TimeRangeWidget — min/max in years+months, outputs total months
// ============================================================
class TimeRangeWidget {
    constructor(container, { label = '', min = null, max = null, onChange = null } = {}) {
        this.container = typeof container === 'string' ? document.querySelector(container) : container;
        this.label = label;
        this.minMonths = min;
        this.maxMonths = max;
        this.onChange = onChange;
        this.render();
    }

    render() {
        this.container.innerHTML = '';
        const wrapper = document.createElement('div');
        wrapper.className = 'space-y-1';

        if (this.label) {
            const lbl = document.createElement('label');
            lbl.className = 'block text-xs font-medium text-gray-600';
            lbl.textContent = this.label;
            wrapper.appendChild(lbl);
        }

        const row = document.createElement('div');
        row.className = 'flex items-center gap-2 text-sm';

        // Min
        row.appendChild(this.makeLabel('Min'));
        const minYears = this.makeNumInput('Yr', Math.floor((this.minMonths || 0) / 12) || '');
        const minMo = this.makeNumInput('Mo', (this.minMonths || 0) % 12 || '');
        row.appendChild(minYears);
        row.appendChild(minMo);

        row.appendChild(this.makeLabel('Max'));
        const maxYears = this.makeNumInput('Yr', Math.floor((this.maxMonths || 0) / 12) || '');
        const maxMo = this.makeNumInput('Mo', (this.maxMonths || 0) % 12 || '');
        row.appendChild(maxYears);
        row.appendChild(maxMo);

        const update = () => {
            const minVal = (parseInt(minYears.value) || 0) * 12 + (parseInt(minMo.value) || 0);
            const maxVal = (parseInt(maxYears.value) || 0) * 12 + (parseInt(maxMo.value) || 0);
            this.minMonths = minVal || null;
            this.maxMonths = maxVal || null;
            if (this.onChange) this.onChange(this.getValues());
        };
        [minYears, minMo, maxYears, maxMo].forEach(el => el.addEventListener('input', update));

        wrapper.appendChild(row);
        this.container.appendChild(wrapper);
    }

    makeLabel(text) {
        const s = document.createElement('span');
        s.className = 'text-xs text-gray-500 whitespace-nowrap';
        s.textContent = text;
        return s;
    }

    makeNumInput(placeholder, value) {
        const input = document.createElement('input');
        input.type = 'number';
        input.min = '0';
        input.placeholder = placeholder;
        input.className = 'border rounded px-1.5 py-1 w-12 text-sm text-center';
        if (value) input.value = value;
        return input;
    }

    getValues() {
        const result = {};
        if (this.minMonths) result.min = this.minMonths;
        if (this.maxMonths) result.max = this.maxMonths;
        return Object.keys(result).length ? result : null;
    }
}
