/* Light, dark, or whatever the system says.
 *
 * The choice is one key in localStorage, so it survives a refresh and is shared
 * by every page of the site — including the figure, which reads the same key from
 * its own document. With no choice stored, the system decides and keeps deciding
 * as the system changes; once a person picks, their pick wins.
 *
 * The attribute is set once more in <head> before the stylesheet loads, so a page
 * never flashes the wrong theme on the way in.
 */
(function () {
  var KEY = "moontask-theme";
  var root = document.documentElement;
  var media = matchMedia("(prefers-color-scheme: dark)");

  function stored() {
    try { return localStorage.getItem(KEY); } catch (e) { return null; }
  }

  function dark(v) {
    return v === "dark" || (v !== "light" && media.matches);
  }

  function apply(v) {
    if (v === "light" || v === "dark") root.setAttribute("data-theme", v);
    else root.removeAttribute("data-theme");
    root.style.colorScheme = dark(v) ? "dark" : "light";
    var on = dark(v) ? "true" : "false";
    for (var b of document.querySelectorAll(".theme-toggle")) {
      b.setAttribute("aria-pressed", on);
      b.title = dark(v) ? "Switch to the light theme" : "Switch to the dark theme";
    }
  }

  function set(v) {
    try { localStorage.setItem(KEY, v); } catch (e) { /* private mode: still applies now */ }
    apply(v);
  }

  window.moontaskTheme = {
    value: stored,
    dark: function () { return dark(stored()); },
    set: set,
    toggle: function () { set(dark(stored()) ? "light" : "dark"); },
  };

  addEventListener("storage", function (e) { if (e.key === KEY) apply(e.newValue); });
  media.addEventListener("change", function () { if (!stored()) apply(null); });
  apply(stored());

  addEventListener("DOMContentLoaded", function () {
    for (var b of document.querySelectorAll(".theme-toggle"))
      b.addEventListener("click", function () { window.moontaskTheme.toggle(); });
    apply(stored());
  });
})();
