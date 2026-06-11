(function () {
  document.querySelectorAll("[data-ws-share-root]").forEach(function (root) {
    var input = root.querySelector("[data-ws-share-input]");
    var copyBtn = root.querySelector("[data-ws-share-copy]");
    var copiedEl = root.querySelector("[data-ws-share-copied]");
    if (!input || !copyBtn) return;

    copyBtn.addEventListener("click", function () {
      var url = (input.value || "").trim();
      if (!url) return;

      function done() {
        copyBtn.textContent = "Copied!";
        if (copiedEl) copiedEl.classList.remove("hidden");
        setTimeout(function () {
          copyBtn.textContent = "Copy link";
          if (copiedEl) copiedEl.classList.add("hidden");
        }, 2200);
      }

      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(url).then(done).catch(function () {
          input.select();
          try {
            document.execCommand("copy");
            done();
          } catch (e) {}
        });
        return;
      }

      input.select();
      try {
        document.execCommand("copy");
        done();
      } catch (e) {}
    });
  });
})();
