(function () {
  "use strict";

  var COPY_RESET_MS = 1600;

  function legacyCopy(text) {
    var area = document.createElement("textarea");
    area.className = "clipboard-fallback";
    area.value = text;
    area.setAttribute("readonly", "");
    document.body.appendChild(area);
    area.select();
    var ok = false;
    try {
      ok = document.execCommand("copy");
    } catch (error) {
      ok = false;
    }
    area.remove();
    return ok;
  }

  function copy(text) {
    if (navigator.clipboard && window.isSecureContext) {
      return navigator.clipboard.writeText(text).then(
        function () {
          return true;
        },
        function () {
          return legacyCopy(text);
        }
      );
    }
    return Promise.resolve(legacyCopy(text));
  }

  function flash(button, ok) {
    if (!button.dataset.label) {
      button.dataset.label = button.textContent;
    }
    window.clearTimeout(button.copyTimer);
    button.textContent = ok ? "Copied" : "Copy failed";
    if (ok) {
      button.setAttribute("data-copied", "");
    } else {
      button.removeAttribute("data-copied");
    }
    button.copyTimer = window.setTimeout(function () {
      button.textContent = button.dataset.label;
      button.removeAttribute("data-copied");
    }, COPY_RESET_MS);
  }

  function wire(button, code) {
    button.hidden = false;
    button.addEventListener("click", function () {
      copy(code.innerText.replace(/\s+$/, "")).then(function (ok) {
        flash(button, ok);
      });
    });
  }

  document.querySelectorAll("[data-copy]").forEach(function (button) {
    var scope = button.closest("figure");
    var code = scope && scope.querySelector("code");
    if (code) {
      wire(button, code);
    }
  });

  document.querySelectorAll("pre").forEach(function (pre) {
    if (pre.closest(".terminal")) {
      return;
    }
    var code = pre.querySelector("code") || pre;
    var wrap = document.createElement("div");
    wrap.className = "pre-wrap";
    pre.parentNode.insertBefore(wrap, pre);
    wrap.appendChild(pre);

    var button = document.createElement("button");
    button.type = "button";
    button.className = "copy-btn";
    button.textContent = "Copy";
    button.setAttribute("aria-label", "Copy code to clipboard");
    wrap.appendChild(button);
    wire(button, code);
  });

  var menu = document.querySelector("details.menu");
  if (menu) {
    menu.querySelectorAll("a").forEach(function (link) {
      link.addEventListener("click", function () {
        menu.removeAttribute("open");
      });
    });

    document.addEventListener("keydown", function (event) {
      if (event.key === "Escape" && menu.open) {
        menu.removeAttribute("open");
        var summary = menu.querySelector("summary");
        if (summary) {
          summary.focus();
        }
      }
    });

    document.addEventListener("click", function (event) {
      if (menu.open && !menu.contains(event.target)) {
        menu.removeAttribute("open");
      }
    });
  }
})();
