st.markdown("""
<style>
    .sidebar-toggle {
        position: fixed;
        left: 0;
        top: 50%;
        transform: translateY(-50%);
        background: #00c9ff;
        color: #07101e;
        width: 30px;
        height: 70px;
        border-radius: 0 8px 8px 0;
        display: flex;
        align-items: center;
        justify-content: center;
        cursor: pointer;
        z-index: 999999;
        font-size: 18px;
        font-weight: bold;
        transition: all 0.2s;
        border: none;
        box-shadow: 2px 0 5px rgba(0,0,0,0.2);
    }
    .sidebar-toggle:hover {
        background: #0a85c2;
        width: 36px;
    }
</style>

<div class="sidebar-toggle" id="sidebarToggle">▶</div>

<script>
    (function() {
        function findCollapseButton() {
            // Try multiple selectors that Streamlit might use
            const selectors = [
                'button[data-testid="baseButton-headerNoPadding"]',
                'button[kind="header"]',
                '[data-testid="stSidebarCollapseButton"]',
                '.st-emotion-cache-1v7f65g button',
                'button[aria-label="Collapse sidebar"]'
            ];
            
            for (const selector of selectors) {
                const btn = document.querySelector(selector);
                if (btn) return btn;
            }
            return null;
        }
        
        function toggleSidebar() {
            const collapseBtn = findCollapseButton();
            if (collapseBtn) {
                collapseBtn.click();
                setTimeout(function() {
                    const isCollapsed = collapseBtn.getAttribute('aria-expanded') === 'false';
                    const toggleBtn = document.getElementById('sidebarToggle');
                    if (toggleBtn) {
                        toggleBtn.innerHTML = isCollapsed ? '▶' : '◀';
                    }
                }, 300);
            } else {
                // Fallback: try to force sidebar via CSS
                const sidebar = document.querySelector('section[data-testid="stSidebar"]');
                if (sidebar) {
                    sidebar.style.transform = 'translateX(0px)';
                    sidebar.style.width = '330px';
                }
            }
        }
        
        const toggleBtn = document.getElementById('sidebarToggle');
        if (toggleBtn) {
            toggleBtn.onclick = toggleSidebar;
        }
        
        // Initial arrow direction
        setTimeout(function() {
            const collapseBtn = findCollapseButton();
            const toggleBtn = document.getElementById('sidebarToggle');
            if (collapseBtn && toggleBtn) {
                const isCollapsed = collapseBtn.getAttribute('aria-expanded') === 'false';
                toggleBtn.innerHTML = isCollapsed ? '▶' : '◀';
            }
        }, 500);
    })();
</script>
""", unsafe_allow_html=True)
