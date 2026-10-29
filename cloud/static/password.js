(function () {
  document.querySelectorAll(".pw-toggle").forEach(function (btn) {
    var field = btn.parentElement;
    var input = field && field.querySelector("input");
    if (!input) return;
    btn.addEventListener("click", function () {
      var show = input.type === "password";
      input.type = show ? "text" : "password";
      btn.classList.toggle("is-on", show);
      btn.setAttribute("aria-pressed", show ? "true" : "false");
      btn.setAttribute("aria-label", show ? "Hide password" : "Show password");
    });
  });
})();
