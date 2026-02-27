/**
 * PRG Risk Intelligence — UI Micro-Interactions
 * ui.js v2.0
 *
 * Handles visual polish that sits on top of the main app logic:
 *   • Ripple effect on buttons & filter chips
 *   • Number counter animations for KPI cards
 *   • Smooth toast notifications
 *   • Skeleton loaders
 *   • Tooltip positioning fixes on small screens
 */

(function () {
  'use strict';

  /* ── RIPPLE ─────────────────────────────────────────────── */
  function createRipple(e) {
    const btn = e.currentTarget;
    const existing = btn.querySelector('.ui-ripple');
    if (existing) existing.remove();

    const circle  = document.createElement('span');
    const rect    = btn.getBoundingClientRect();
    const size    = Math.max(rect.width, rect.height);
    const clientX = e.touches ? e.touches[0].clientX : e.clientX;
    const clientY = e.touches ? e.touches[0].clientY : e.clientY;

    circle.className = 'ui-ripple';
    Object.assign(circle.style, {
      position:    'absolute',
      borderRadius:'50%',
      width:       size + 'px',
      height:      size + 'px',
      left:        (clientX - rect.left - size / 2) + 'px',
      top:         (clientY - rect.top  - size / 2) + 'px',
      background:  'rgba(255,255,255,0.15)',
      pointerEvents:'none',
      transform:   'scale(0)',
      animation:   'ui-ripple-kf 0.5s linear',
    });

    btn.style.position = 'relative';
    btn.style.overflow = 'hidden';
    btn.appendChild(circle);
    circle.addEventListener('animationend', () => circle.remove());
  }

  function attachRipples() {
    document.querySelectorAll(
      '.btn-reset, .btn-sample, .filter-btn, .tab, ' +
      'button[id^="btn-"]'
    ).forEach(el => {
      if (el.dataset.rippleAttached) return;
      el.dataset.rippleAttached = '1';
      el.addEventListener('mousedown', createRipple);
    });
  }

  // Inject ripple keyframes once
  const style = document.createElement('style');
  style.textContent = `
    @keyframes ui-ripple-kf {
      to { transform: scale(3); opacity: 0; }
    }
    .kpi-value[data-counting] { transition: none; }
  `;
  document.head.appendChild(style);

  /* ── COUNTER ANIMATION ──────────────────────────────────── */
  function animateCounter(el, target, duration) {
    if (isNaN(target)) return;
    const start = performance.now();
    const from  = 0;

    function step(now) {
      const progress = Math.min((now - start) / duration, 1);
      const ease = 1 - Math.pow(1 - progress, 3);
      el.textContent = Math.round(from + (target - from) * ease);
      if (progress < 1) requestAnimationFrame(step);
      else el.textContent = target;
    }
    requestAnimationFrame(step);
  }

  function runCounters() {
    document.querySelectorAll('.kpi-value:not([data-animated])').forEach(el => {
      const raw = parseFloat(el.textContent.replace(/[^0-9.]/g, ''));
      if (!isNaN(raw) && raw > 0 && el.textContent === String(Math.round(raw))) {
        el.dataset.animated = '1';
        animateCounter(el, raw, 700);
      }
    });
  }

  /* ── TOAST NOTIFICATIONS ────────────────────────────────── */
  let toastContainer = null;

  function getToastContainer() {
    if (!toastContainer) {
      toastContainer = document.createElement('div');
      Object.assign(toastContainer.style, {
        position: 'fixed',
        bottom:   '1.5rem',
        right:    '1.5rem',
        zIndex:   '9999',
        display:  'flex',
        flexDirection: 'column-reverse',
        gap:      '0.5rem',
        pointerEvents: 'none',
      });
      document.body.appendChild(toastContainer);
    }
    return toastContainer;
  }

  window.showToast = function (message, type /* 'success'|'error'|'info' */ = 'info', duration = 3500) {
    const colors = {
      success: { bg: 'rgba(34,197,94,0.12)',  border: 'rgba(34,197,94,0.35)',  color: '#86efac' },
      error:   { bg: 'rgba(244,63,94,0.12)',  border: 'rgba(244,63,94,0.35)',  color: '#fda4af' },
      info:    { bg: 'rgba(96,165,250,0.1)',  border: 'rgba(96,165,250,0.3)',  color: '#93c5fd' },
    };

    const { bg, border, color } = colors[type] || colors.info;

    const toast = document.createElement('div');
    Object.assign(toast.style, {
      background:   bg,
      border:       `1px solid ${border}`,
      borderRadius: '10px',
      padding:      '0.7rem 1.1rem',
      color:        color,
      fontFamily:   'var(--font-mono, monospace)',
      fontSize:     '0.72rem',
      backdropFilter: 'blur(12px)',
      boxShadow:    '0 4px 24px rgba(0,0,0,0.5)',
      maxWidth:     '320px',
      lineHeight:   '1.5',
      pointerEvents:'auto',
      opacity:      '0',
      transform:    'translateY(8px)',
      transition:   'opacity 0.2s, transform 0.2s',
    });
    toast.textContent = message;

    getToastContainer().appendChild(toast);
    requestAnimationFrame(() => {
      toast.style.opacity   = '1';
      toast.style.transform = 'translateY(0)';
    });

    setTimeout(() => {
      toast.style.opacity   = '0';
      toast.style.transform = 'translateY(8px)';
      toast.addEventListener('transitionend', () => toast.remove(), { once: true });
    }, duration);
  };

  /* ── SKELETON LOADERS ───────────────────────────────────── */
  const skeletonStyles = `
    .skeleton {
      background: linear-gradient(
        90deg,
        rgba(255,255,255,0.04) 0%,
        rgba(255,255,255,0.09) 50%,
        rgba(255,255,255,0.04) 100%
      );
      background-size: 200% 100%;
      animation: shimmer 1.4s infinite;
      border-radius: 4px;
    }
    @keyframes shimmer {
      0%   { background-position: 200% 0; }
      100% { background-position:-200% 0; }
    }
  `;
  const skStyle = document.createElement('style');
  skStyle.textContent = skeletonStyles;
  document.head.appendChild(skStyle);

  /* ── SMOOTH SECTION TRANSITIONS ────────────────────────── */
  function observeTabContent() {
    const observer = new IntersectionObserver(entries => {
      entries.forEach(entry => {
        if (entry.isIntersecting) {
          entry.target.style.animationPlayState = 'running';
        }
      });
    }, { threshold: 0.02 });

    document.querySelectorAll('.kpi-card, .chart-card, .contractor-card').forEach(el => {
      el.style.animationPlayState = 'paused';
      observer.observe(el);
    });
  }

  /* ── TOPBAR SCROLL SHADOW ───────────────────────────────── */
  function attachScrollShadow() {
    const topbar = document.querySelector('.topbar');
    if (!topbar) return;
    window.addEventListener('scroll', () => {
      if (window.scrollY > 4) {
        topbar.style.boxShadow = '0 2px 20px rgba(0,0,0,0.6), 0 1px 0 rgba(34,197,94,0.06)';
      } else {
        topbar.style.boxShadow = '';
      }
    }, { passive: true });
  }

  /* ── TABLE ROW MICRO-ANIMATION ──────────────────────────── */
  function animateTableRows(tbody) {
    if (!tbody) return;
    Array.from(tbody.rows).forEach((row, i) => {
      row.style.opacity   = '0';
      row.style.transform = 'translateY(6px)';
      row.style.transition = `opacity 0.18s ease ${i * 0.025}s, transform 0.18s ease ${i * 0.025}s`;
      requestAnimationFrame(() => requestAnimationFrame(() => {
        row.style.opacity   = '1';
        row.style.transform = 'translateY(0)';
      }));
    });
  }

  window.uiAnimateTableRows = animateTableRows;

  /* ── INIT ───────────────────────────────────────────────── */
  function init() {
    attachRipples();
    attachScrollShadow();
    observeTabContent();

    // Re-attach ripples whenever DOM mutates (tables re-rendered, etc.)
    const mo = new MutationObserver(() => {
      attachRipples();
      runCounters();
    });
    mo.observe(document.body, { childList: true, subtree: true });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
