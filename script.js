(() => {
  const loader = document.getElementById("pageLoader");
  if (!loader) {
    return;
  }

  const minDurationMs = 1500;
  const startedAt = performance.now();
  let isWindowLoaded = document.readyState === "complete";

  const hideLoader = () => {
    if (!isWindowLoaded) {
      return;
    }

    const elapsed = performance.now() - startedAt;
    const remaining = Math.max(0, minDurationMs - elapsed);

    window.setTimeout(() => {
      loader.classList.add("is-hidden");
      loader.addEventListener(
        "transitionend",
        () => {
          loader.remove();
        },
        { once: true }
      );
    }, remaining);
  };

  if (!isWindowLoaded) {
    window.addEventListener(
      "load",
      () => {
        isWindowLoaded = true;
        hideLoader();
      },
      { once: true }
    );
  } else {
    hideLoader();
  }
})();

(() => {
  const webApp = window.Telegram?.WebApp;
  if (!webApp) {
    return;
  }

  webApp.ready();
  webApp.expand();
})();

(() => {
  const panel = document.querySelector(".main-glass-panel");
  const tabs = document.querySelectorAll(".panel-tab");
  const views = document.querySelectorAll(".panel-view");
  if (!panel || !tabs.length || !views.length) {
    return;
  }

  const panelClasses = ["is-stars", "is-premium", "is-ton"];

  const activatePanel = (name) => {
    panel.classList.remove(...panelClasses);
    panel.classList.add(`is-${name}`);

    tabs.forEach((tab) => {
      const isActive = tab.dataset.panel === name;
      tab.classList.toggle("is-active", isActive);
      tab.setAttribute("aria-selected", isActive ? "true" : "false");
    });

    views.forEach((view) => {
      view.classList.toggle("is-active", view.dataset.view === name);
    });
  };

  tabs.forEach((tab) => {
    tab.addEventListener("click", () => {
      activatePanel(tab.dataset.panel);
    });
  });
})();

(() => {
  const dock = document.getElementById("appDock");
  const dockOrb = document.getElementById("appDockOrb");
  const dockButtons = Array.from(document.querySelectorAll(".app-dock-button"));
  const screens = Array.from(document.querySelectorAll(".app-screen"));
  if (!dock || !dockOrb || !dockButtons.length || !screens.length) {
    return;
  }

  let selectedScreen = "home";
  let activePointerId = null;
  let isDragging = false;
  let lastPointerX = 0;
  let pendingPointerX = null;
  let pointerFrameId = null;

  const clamp = (value, min, max) => Math.min(max, Math.max(min, value));

  const getDockBounds = () => dock.getBoundingClientRect();

  const buildLabelGlyphs = (labelNode) => {
    if (!(labelNode instanceof HTMLElement)) {
      return;
    }

    const hasGlyphMarkup = Boolean(labelNode.querySelector(".dock-glyph"));
    const rawText = hasGlyphMarkup
      ? labelNode.dataset.rawLabelText || labelNode.textContent || ""
      : labelNode.textContent || "";
    labelNode.dataset.rawLabelText = rawText;
    labelNode.textContent = "";

    Array.from(rawText).forEach((char) => {
      const glyph = document.createElement("span");
      glyph.className = "dock-glyph";
      glyph.style.setProperty("--glyph-scale-x", "1");
      glyph.style.setProperty("--glyph-skew-x", "0deg");
      if (char === " ") {
        glyph.classList.add("dock-glyph--space");
        glyph.textContent = "\u00A0";
      } else {
        glyph.textContent = char;
      }
      labelNode.appendChild(glyph);
    });
  };

  const syncDockLabelGlyphs = () => {
    dock.querySelectorAll(".app-dock-label").forEach((label) => {
      buildLabelGlyphs(label);
    });
  };

  const setGlyphDistortionForLabel = (labelNode, lensRect) => {
    if (!(labelNode instanceof HTMLElement)) {
      return;
    }

    const glyphNodes = labelNode.querySelectorAll(".dock-glyph");
    glyphNodes.forEach((glyphNode) => {
      const glyphRect = glyphNode.getBoundingClientRect();
      const glyphWidth = Math.max(glyphRect.width, 1);
      const overlapLeft = Math.max(glyphRect.left, lensRect.left);
      const overlapRight = Math.min(glyphRect.right, lensRect.right);
      const overlapWidth = Math.max(0, overlapRight - overlapLeft);

      if (overlapWidth <= 0) {
        glyphNode.style.setProperty("--glyph-scale-x", "1");
        glyphNode.style.setProperty("--glyph-skew-x", "0deg");
        return;
      }

      const glyphCenter = glyphRect.left + glyphRect.width / 2;
      const nearestEdgeDistance = Math.min(
        Math.abs(glyphCenter - lensRect.left),
        Math.abs(glyphCenter - lensRect.right)
      );

      const coverage = overlapWidth / glyphWidth;
      const edgeRange = Math.max(6, glyphWidth * 2.6);
      const edgeInfluence = clamp(1 - nearestEdgeDistance / edgeRange, 0, 1);
      const partialCoverage = Math.sin(Math.PI * clamp(coverage, 0, 1));
      const overlapCenter = overlapLeft + overlapWidth / 2;
      const centerOffset = clamp((overlapCenter - glyphCenter) / (glyphWidth / 2), -1, 1);
      const stretch = 1 + partialCoverage * edgeInfluence * 1.18;
      const skewX = centerOffset * partialCoverage * edgeInfluence * 20;

      glyphNode.style.setProperty("--glyph-scale-x", stretch.toFixed(3));
      glyphNode.style.setProperty("--glyph-skew-x", `${skewX.toFixed(3)}deg`);
    });
  };

  const resetGlyphDistortionForButton = (button) => {
    button.querySelectorAll(".dock-glyph").forEach((glyphNode) => {
      glyphNode.style.setProperty("--glyph-scale-x", "1");
      glyphNode.style.setProperty("--glyph-skew-x", "0deg");
    });
  };

  const getButtonCenterX = (button) => {
    const rect = button.getBoundingClientRect();
    return rect.left + rect.width / 2;
  };

  const setOrbPosition = (clientX) => {
    const bounds = getDockBounds();
    const orbRect = dockOrb.getBoundingClientRect();
    const halfOrbWidth = orbRect.width / 2;
    const minX = Math.min(halfOrbWidth, bounds.width / 2);
    const maxX = Math.max(bounds.width - halfOrbWidth, bounds.width / 2);
    const normalized = clamp(clientX - bounds.left, minX, maxX);
    dockOrb.style.setProperty("--orb-x", `${normalized}px`);
    lastPointerX = clientX;
  };

  const setLensClipForNode = (targetNode, lensRect) => {
    if (!(targetNode instanceof Element)) {
      return;
    }

    const targetRect = targetNode.getBoundingClientRect();
    const overlapLeft = Math.max(targetRect.left, lensRect.left);
    const overlapRight = Math.min(targetRect.right, lensRect.right);
    const overlapWidth = Math.max(0, overlapRight - overlapLeft);

    if (overlapWidth <= 0 || targetRect.width <= 0) {
      targetNode.style.setProperty("--lens-clip-start", "0%");
      targetNode.style.setProperty("--lens-clip-end", "0%");
      targetNode.style.setProperty("--lens-edge-left", "0");
      targetNode.style.setProperty("--lens-edge-right", "0");
      return;
    }

    const startPercent = ((overlapLeft - targetRect.left) / targetRect.width) * 100;
    const endPercent = ((overlapRight - targetRect.left) / targetRect.width) * 100;
    targetNode.style.setProperty("--lens-clip-start", `${startPercent.toFixed(3)}%`);
    targetNode.style.setProperty("--lens-clip-end", `${endPercent.toFixed(3)}%`);

    const edgeRange = Math.max(8, targetRect.width * 0.3);
    const leftInside = lensRect.left >= targetRect.left && lensRect.left <= targetRect.right;
    const rightInside = lensRect.right >= targetRect.left && lensRect.right <= targetRect.right;

    let leftPull = 0;
    let rightPull = 0;

    if (leftInside) {
      const leftDistance = Math.abs(lensRect.left - targetRect.left);
      leftPull = Math.max(0, Math.min(1, 1 - leftDistance / edgeRange));
    }

    if (rightInside) {
      const rightDistance = Math.abs(targetRect.right - lensRect.right);
      rightPull = Math.max(0, Math.min(1, 1 - rightDistance / edgeRange));
    }

    targetNode.style.setProperty("--lens-edge-left", leftPull.toFixed(3));
    targetNode.style.setProperty("--lens-edge-right", rightPull.toFixed(3));
  };

  const resetLensClip = (button) => {
    const iconWrap = button.querySelector(".app-dock-icon-wrap");
    const labelWrap = button.querySelector(".app-dock-label-wrap");
    iconWrap?.style.setProperty("--lens-clip-start", "0%");
    iconWrap?.style.setProperty("--lens-clip-end", "0%");
    iconWrap?.style.setProperty("--lens-edge-left", "0");
    iconWrap?.style.setProperty("--lens-edge-right", "0");
    labelWrap?.style.setProperty("--lens-clip-start", "0%");
    labelWrap?.style.setProperty("--lens-clip-end", "0%");
    labelWrap?.style.setProperty("--lens-edge-left", "0");
    labelWrap?.style.setProperty("--lens-edge-right", "0");
  };

  const setButtonHighlightByX = (clientX) => {
    const lensRect = dockOrb.getBoundingClientRect();

    dockButtons.forEach((button) => {
      const buttonRect = button.getBoundingClientRect();
      const centerX = getButtonCenterX(button);
      const distance = Math.abs(centerX - clientX);
      const radius = Math.max(buttonRect.width * 1.2, 84);
      const intensity = Math.max(0, Math.min(1, 1 - distance / radius));
      button.style.setProperty("--dock-lens", intensity.toFixed(3));

      setLensClipForNode(button.querySelector(".app-dock-icon-wrap"), lensRect);
      setLensClipForNode(button.querySelector(".app-dock-label-wrap"), lensRect);
      button.querySelectorAll(".app-dock-label").forEach((labelNode) => {
        setGlyphDistortionForLabel(labelNode, lensRect);
      });
    });
  };

  const applyPointerPosition = (clientX) => {
    setOrbPosition(clientX);
    setButtonHighlightByX(clientX);
  };

  const flushPointerFrame = () => {
    pointerFrameId = null;
    if (!isDragging || pendingPointerX === null) {
      return;
    }
    applyPointerPosition(pendingPointerX);
  };

  const queuePointerFrame = (clientX) => {
    pendingPointerX = clientX;
    if (pointerFrameId !== null) {
      return;
    }
    pointerFrameId = window.requestAnimationFrame(flushPointerFrame);
  };

  const setSelectedButton = (targetScreen) => {
    selectedScreen = targetScreen;
    dockButtons.forEach((button) => {
      const isSelected = button.dataset.target === targetScreen;
      button.classList.toggle("is-selected", isSelected);
      if (isSelected) {
        button.setAttribute("aria-current", "page");
      } else {
        button.removeAttribute("aria-current");
      }
      button.style.setProperty("--dock-lens", "0");
      resetLensClip(button);
      resetGlyphDistortionForButton(button);
    });
  };

  const scrollToPageStart = () => {
    window.scrollTo({ top: 0, left: 0, behavior: "auto" });
    if (document.documentElement) {
      document.documentElement.scrollTop = 0;
    }
    if (document.body) {
      document.body.scrollTop = 0;
    }
  };

  const setActiveScreen = (targetScreen) => {
    const knownScreen = screens.some((screen) => screen.dataset.screen === targetScreen);
    const nextScreen = knownScreen ? targetScreen : "home";

    screens.forEach((screen) => {
      const isActive = screen.dataset.screen === nextScreen;
      screen.classList.toggle("is-active", isActive);
      screen.setAttribute("aria-hidden", isActive ? "false" : "true");
    });

    setSelectedButton(nextScreen);
    scrollToPageStart();
  };

  const findClosestButton = (clientX) => {
    let closestButton = dockButtons[0];
    let smallestDistance = Number.POSITIVE_INFINITY;

    dockButtons.forEach((button) => {
      const distance = Math.abs(getButtonCenterX(button) - clientX);
      if (distance < smallestDistance) {
        smallestDistance = distance;
        closestButton = button;
      }
    });

    return closestButton;
  };

  const alignOrbToSelected = () => {
    const selectedButton =
      dockButtons.find((button) => button.dataset.target === selectedScreen) || dockButtons[0];
    setOrbPosition(getButtonCenterX(selectedButton));
  };

  const finishDragging = () => {
    if (!isDragging) {
      return;
    }

    isDragging = false;
    dock.classList.remove("is-dragging");

    const closestButton = findClosestButton(lastPointerX);
    setActiveScreen(closestButton.dataset.target || "home");
    alignOrbToSelected();

    dockButtons.forEach((button) => {
      button.style.setProperty("--dock-lens", "0");
      resetLensClip(button);
      resetGlyphDistortionForButton(button);
    });

    if (pointerFrameId !== null) {
      window.cancelAnimationFrame(pointerFrameId);
      pointerFrameId = null;
    }
    pendingPointerX = null;
    activePointerId = null;
  };

  dockButtons.forEach((button) => {
    button.addEventListener("click", (event) => {
      if (isDragging) {
        event.preventDefault();
        return;
      }
      setActiveScreen(button.dataset.target || "home");
      alignOrbToSelected();
    });
  });

  dock.addEventListener("pointerdown", (event) => {
    if (!(event.target instanceof Element)) {
      return;
    }
    if (!event.target.closest(".app-dock-button")) {
      return;
    }

    isDragging = true;
    activePointerId = event.pointerId;
    dock.classList.add("is-dragging");
    applyPointerPosition(event.clientX);
    dock.setPointerCapture?.(event.pointerId);
    event.preventDefault();
  });

  dock.addEventListener("pointermove", (event) => {
    if (!isDragging || (activePointerId !== null && event.pointerId !== activePointerId)) {
      return;
    }

    queuePointerFrame(event.clientX);
  });

  dock.addEventListener("pointerup", (event) => {
    if (activePointerId !== null && event.pointerId !== activePointerId) {
      return;
    }
    if (isDragging) {
      if (pointerFrameId !== null) {
        window.cancelAnimationFrame(pointerFrameId);
        pointerFrameId = null;
      }
      pendingPointerX = event.clientX;
      applyPointerPosition(event.clientX);
    }
    finishDragging();
  });

  dock.addEventListener("pointercancel", finishDragging);
  dock.addEventListener("lostpointercapture", finishDragging);

  window.addEventListener("starslix:navigate", (event) => {
    const targetScreen = event?.detail?.screen;
    if (typeof targetScreen !== "string") {
      return;
    }
    setActiveScreen(targetScreen);
    alignOrbToSelected();
  });

  window.addEventListener("starslix:dock-labels-updated", syncDockLabelGlyphs);

  syncDockLabelGlyphs();
  setActiveScreen("home");
  alignOrbToSelected();
})();

(() => {
  const grids = document.querySelectorAll(".offer-grid");
  if (!grids.length) {
    return;
  }

  const defaultUsdRubRate = 76.5;
  const defaultStarRates = {
    "50_75": 1.7,
    "76_100": 1.6,
    "101_250": 1.55,
    "251_plus": 1.5,
  };

  const getMiniappPricingConfig = () => window.starslixMiniappConfig || {};
  const getUsdRubRate = () => {
    const numeric = Number(getMiniappPricingConfig().usdRubRate);
    return Number.isFinite(numeric) && numeric > 0 ? numeric : defaultUsdRubRate;
  };
  const getStarRates = () => ({
    ...defaultStarRates,
    ...(getMiniappPricingConfig().starRates || {}),
  });
  const roundToTwo = (value) => Math.round(Number(value) * 100) / 100;

  const getStarsRateForAmount = (starsCount) => {
    const rates = getStarRates();
    if (starsCount >= 50 && starsCount <= 75) {
      return Number(rates["50_75"]) || defaultStarRates["50_75"];
    }
    if (starsCount >= 76 && starsCount <= 100) {
      return Number(rates["76_100"]) || defaultStarRates["76_100"];
    }
    if (starsCount >= 101 && starsCount <= 250) {
      return Number(rates["101_250"]) || defaultStarRates["101_250"];
    }
    return Number(rates["251_plus"]) || defaultStarRates["251_plus"];
  };

  const formatRubValue = (value) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return "—";
    }
    return `${Number.isInteger(numeric) ? numeric : numeric.toFixed(2)}₽`;
  };

  const formatUsdValue = (value) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return "—";
    }
    return `${numeric.toFixed(2)}$`;
  };

  const getPremiumPriceFromConfig = (monthsValue) => {
    const premiumOffers = Array.isArray(getMiniappPricingConfig().premium)
      ? getMiniappPricingConfig().premium
      : [];
    const match = premiumOffers.find((offer) => Number(offer?.months) === Number(monthsValue));
    if (!match) {
      return null;
    }
    return {
      usd: formatUsdValue(match.usd),
      rub: formatRubValue(match.rub),
    };
  };

  grids.forEach((grid) => {
    const cards = Array.from(grid.querySelectorAll(".offer-card"));
    const panelView = grid.closest(".panel-view");
    const quantityInput = panelView?.querySelector('.form-input[type="number"]');
    const priceUsdNode = panelView?.querySelector("[data-price-usd]");
    const priceRubNode = panelView?.querySelector("[data-price-rub]");

    const getCardAmount = (card) =>
      card.querySelector(".offer-card-amount")?.textContent?.trim() || "";

    const getCardPrice = (card) => {
      const rawPrice = card?.querySelector(".offer-card-price")?.textContent?.trim() || "";
      const [usdPart = "", rubPart = ""] = rawPrice.split("/");
      return {
        usd: usdPart.trim(),
        rub: rubPart.trim(),
      };
    };

    const syncPriceRow = (card) => {
      if (!priceUsdNode || !priceRubNode) {
        return;
      }

      const isStarsView = panelView?.dataset?.view === "stars";
      if (isStarsView && quantityInput) {
        const amount = Number.parseInt(String(quantityInput.value || "").trim(), 10);
        if (Number.isFinite(amount) && amount >= 50 && amount <= 10000) {
          const rub = roundToTwo(amount * getStarsRateForAmount(amount));
          const usd = roundToTwo(rub / getUsdRubRate());
          priceUsdNode.textContent = formatUsdValue(usd);
          priceRubNode.textContent = formatRubValue(rub);
          return;
        }
      }

      const sourceCard =
        card || cards.find((item) => item.classList.contains("is-active")) || null;
      if (!sourceCard) {
        priceUsdNode.textContent = "—";
        priceRubNode.textContent = "—";
        return;
      }

      const isPremiumView = panelView?.dataset?.view === "premium";
      if (isPremiumView) {
        const months = Number.parseInt(getCardAmount(sourceCard).replace(/[^0-9]/g, ""), 10);
        if (Number.isFinite(months)) {
          const premiumPrice = getPremiumPriceFromConfig(months);
          if (premiumPrice) {
            priceUsdNode.textContent = premiumPrice.usd;
            priceRubNode.textContent = premiumPrice.rub;
            return;
          }
        }
      }

      const { usd, rub } = getCardPrice(sourceCard);
      priceUsdNode.textContent = usd || "—";
      priceRubNode.textContent = rub || "—";
    };

    const setActiveCard = (activeCard) => {
      cards.forEach((item) => item.classList.remove("is-active"));
      if (activeCard) {
        activeCard.classList.add("is-active");
      }
    };

    const findCardByAmount = (value) => {
      const normalized = String(value).trim();
      if (!normalized) {
        return null;
      }

      return (
        cards.find((card) => getCardAmount(card) === normalized) || null
      );
    };

    const syncInputWithActiveCard = () => {
      const activeCard = cards.find((item) => item.classList.contains("is-active"));
      if (quantityInput) {
        const nextValue = activeCard ? getCardAmount(activeCard) : "";
        if (quantityInput.value !== nextValue) {
          quantityInput.value = nextValue;
        }
      }

      syncPriceRow(activeCard);
    };

    const initialActiveCard = grid.querySelector(".offer-card.is-active");
    if (initialActiveCard) {
      setActiveCard(initialActiveCard);
    }
    syncInputWithActiveCard();

    const refreshGridPrices = () => {
      const activeCard = cards.find((item) => item.classList.contains("is-active")) || null;
      syncPriceRow(activeCard);
    };
    window.addEventListener("starslix:pricing-updated", refreshGridPrices);

    cards.forEach((card) => {
      card.addEventListener("click", () => {
        const shouldDeactivate = card.classList.contains("is-active");
        setActiveCard(shouldDeactivate ? null : card);
        syncInputWithActiveCard();
      });
    });

    if (quantityInput) {
      quantityInput.addEventListener("input", () => {
        const matchedCard = findCardByAmount(quantityInput.value);
        setActiveCard(matchedCard);
        syncPriceRow(matchedCard);
      });
    }
  });
})();

(() => {
  const usernameInputs = document.querySelectorAll(".form-input--telegram");
  const selfFillButtons = document.querySelectorAll(".form-self-fill-button");
  if (!usernameInputs.length && !selfFillButtons.length) {
    return;
  }

  const sanitizeTelegramUsername = (rawValue) => {
    const cleaned = rawValue.replace(/[^@A-Za-z0-9_]/g, "");
    const usernamePart = cleaned.replace(/@/g, "").slice(0, 32);

    if (!cleaned) {
      return "";
    }

    return `@${usernamePart}`;
  };

  usernameInputs.forEach((input) => {
    input.addEventListener("input", () => {
      const sanitizedValue = sanitizeTelegramUsername(input.value);
      if (input.value !== sanitizedValue) {
        input.value = sanitizedValue;
      }
    });
  });

  const getStoredProfileUsername = () => {
    try {
      const raw = window.localStorage.getItem("starslix:profile-user");
      const parsed = raw ? JSON.parse(raw) : null;
      if (!parsed || typeof parsed !== "object") {
        return "";
      }
      return String(parsed.username || "").trim();
    } catch {
      return "";
    }
  };

  const getCurrentTelegramUsername = () => {
    const initUsernameRaw = String(
      window.Telegram?.WebApp?.initDataUnsafe?.user?.username || ""
    ).trim();
    const initUsername = sanitizeTelegramUsername(initUsernameRaw);
    if (initUsername) {
      return initUsername;
    }

    const storedUsername = sanitizeTelegramUsername(getStoredProfileUsername());
    if (storedUsername) {
      return storedUsername;
    }

    return "";
  };

  selfFillButtons.forEach((button) => {
    button.addEventListener("click", (event) => {
      event.preventDefault();
      const wrap = button.closest(".form-input-wrap");
      const input = wrap?.querySelector(".form-input--telegram");
      if (!(input instanceof HTMLInputElement)) {
        return;
      }

      const usernameValue = getCurrentTelegramUsername();
      if (!usernameValue) {
        return;
      }

      input.value = usernameValue;
      input.dispatchEvent(new Event("input", { bubbles: true }));
      input.dispatchEvent(new Event("change", { bubbles: true }));
      input.focus({ preventScroll: true });
    });
  });
})();

(() => {
  const dismissKeyboardIfNeeded = (event) => {
    const activeElement = document.activeElement;
    if (!activeElement) {
      return;
    }

    const isTextInput =
      activeElement.tagName === "INPUT" || activeElement.tagName === "TEXTAREA";
    if (!isTextInput) {
      return;
    }

    const target = event.target;
    if (!(target instanceof Element)) {
      return;
    }

    if (target.closest(".form-input-wrap")) {
      return;
    }

    activeElement.blur();
  };

  document.addEventListener("pointerdown", dismissKeyboardIfNeeded);
  document.addEventListener("touchstart", dismissKeyboardIfNeeded, { passive: true });
})();

(() => {
  const dock = document.getElementById("appDock");
  if (!dock) {
    return;
  }

  const isFormInput = (node) =>
    node instanceof HTMLInputElement || node instanceof HTMLTextAreaElement;

  const updateDockVisibilityForInput = () => {
    const activeElement = document.activeElement;
    const shouldHide =
      isFormInput(activeElement) &&
      activeElement.closest(".main-glass-panel, .panel-view, .form-stack");

    dock.classList.toggle("is-input-hidden", Boolean(shouldHide));
  };

  document.addEventListener("focusin", updateDockVisibilityForInput);
  document.addEventListener("focusout", () => {
    window.setTimeout(updateDockVisibilityForInput, 0);
  });

  window.addEventListener("starslix:navigate", () => {
    window.setTimeout(updateDockVisibilityForInput, 0);
  });
})();

(() => {
  const avatarToggle = document.getElementById("userAvatarToggle");
  const quickMenu = document.getElementById("headerQuickMenu");
  const storePage = document.getElementById("storePage");
  const profilePage = document.getElementById("profilePage");
  const profileCloseButton = document.getElementById("profileCloseButton");
  const profileAvatar = document.getElementById("profileAvatar");
  const profileFullname = document.getElementById("profileFullname");
  const profileUsername = document.getElementById("profileUsername");
  const profileTelegramId = document.getElementById("profileTelegramId");
  const profileHistoryList = document.getElementById("profileHistoryList");
  const profileHistoryEmpty = document.getElementById("profileHistoryEmpty");
  const profileHistoryControls = document.getElementById("profileHistoryControls");
  const profileHistoryPrev = document.getElementById("profileHistoryPrev");
  const profileHistoryNext = document.getElementById("profileHistoryNext");
  const profileHistoryPage = document.getElementById("profileHistoryPage");
  const profileAdminActions = document.getElementById("profileAdminActions");
  const profileAdminButton = document.getElementById("profileAdminButton");
  const appAvatarNodes = document.querySelectorAll(
    ".user-avatar, #profileAvatar, .app-dock-button[data-target=\"profile\"] .app-dock-icon--avatar"
  );
  if (!storePage || !profilePage) {
    return;
  }

  const historyStorageKey = "starslix_purchase_history";
  const historyPageSize = 8;
  let historyPage = 0;
  const historyTypeMeta = {
    stars: {
      icon: "zvezda.png",
    },
    premium: {
      icon: "blackpremicon.png",
    },
    ton: {
      icon: "blactonicon.png",
    },
  };

  const setProfilePageState = (isProfilePage) => {
    document.body.classList.toggle("is-profile-page", isProfilePage);
    if (avatarToggle) {
      avatarToggle.setAttribute("aria-expanded", isProfilePage ? "true" : "false");
    }
    profilePage.setAttribute("aria-hidden", isProfilePage ? "false" : "true");
    storePage.setAttribute("aria-hidden", isProfilePage ? "true" : "false");
  };

  const formatHistoryDate = (isoValue) => {
    const parsedDate = new Date(isoValue);
    if (Number.isNaN(parsedDate.getTime())) {
      return "";
    }
    return parsedDate.toLocaleString();
  };

  const loadHistory = () => {
    try {
      const rawValue = window.localStorage.getItem(historyStorageKey);
      const parsed = rawValue ? JSON.parse(rawValue) : [];
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  };

  const saveHistory = (items) => {
    try {
      window.localStorage.setItem(historyStorageKey, JSON.stringify(items));
    } catch {
      // ignore write errors
    }
  };

  let historyItems = loadHistory();

  const normalizeHistoryType = (item) => {
    const rawType = String(item?.type || "").trim().toLowerCase();
    if (rawType === "stars" || rawType === "premium" || rawType === "ton") {
      return rawType;
    }

    const hint = String(item?.main || "").trim().toLowerCase();
    if (hint.includes("ton")) {
      return "ton";
    }
    if (hint.includes("прем") || hint.includes("premium") || hint.includes("мес")) {
      return "premium";
    }
    return "stars";
  };

  const normalizeHistoryMain = (item, type) => {
    const rawMain = String(item?.main || "").trim();
    if (!rawMain) {
      return "—";
    }

    if (type === "stars") {
      return rawMain.replace(/^(зв[её]зды?|stars?)\s*[:\-]?\s*/i, "").trim() || "—";
    }
    if (type === "premium") {
      return rawMain.replace(/^(премиум|premium)\s*[:\-]?\s*/i, "").trim() || "—";
    }
    return rawMain.replace(/^(ton)\s*[:\-]?\s*/i, "").trim() || "—";
  };

  const updateHistoryPaginationUi = (totalPages) => {
    if (!profileHistoryControls || !profileHistoryPrev || !profileHistoryNext || !profileHistoryPage) {
      return;
    }

    const safePages = Math.max(1, totalPages);
    profileHistoryControls.hidden = safePages <= 1;
    profileHistoryPrev.disabled = historyPage <= 0;
    profileHistoryNext.disabled = historyPage >= safePages - 1;
    profileHistoryPage.textContent = `${historyPage + 1} / ${safePages}`;
  };

  const renderHistory = () => {
    if (!profileHistoryList || !profileHistoryEmpty) {
      return;
    }

    profileHistoryList.querySelectorAll(".profile-history-item").forEach((node) => node.remove());

    if (!historyItems.length) {
      profileHistoryEmpty.hidden = false;
      updateHistoryPaginationUi(1);
      return;
    }

    profileHistoryEmpty.hidden = true;

    const totalPages = Math.max(1, Math.ceil(historyItems.length / historyPageSize));
    historyPage = Math.min(Math.max(0, historyPage), totalPages - 1);
    const pageStart = historyPage * historyPageSize;
    const visibleItems = historyItems.slice(pageStart, pageStart + historyPageSize);

    visibleItems.forEach((item) => {
      const type = normalizeHistoryType(item);
      const iconSrc = historyTypeMeta[type]?.icon || historyTypeMeta.stars.icon;

      const row = document.createElement("div");
      row.className = `profile-history-item profile-history-item--${type}`;

      const iconWrap = document.createElement("span");
      iconWrap.className = "profile-history-icon-wrap";
      iconWrap.setAttribute("aria-hidden", "true");

      const icon = document.createElement("img");
      icon.className = "profile-history-icon";
      icon.src = iconSrc;
      icon.alt = "";
      iconWrap.appendChild(icon);

      const main = document.createElement("span");
      main.className = "profile-history-main";
      main.textContent = normalizeHistoryMain(item, type);

      const price = document.createElement("span");
      price.className = "profile-history-price";
      price.textContent = item.price || "—";

      const date = document.createElement("span");
      date.className = "profile-history-date";
      date.textContent = formatHistoryDate(item.createdAt);

      row.append(iconWrap, main, price, date);
      profileHistoryList.appendChild(row);
    });

    updateHistoryPaginationUi(totalPages);
  };

  const addCurrentOrderToHistory = (override = null) => {
    const activeView = document.querySelector(".panel-view.is-active");
    if (!activeView) {
      return;
    }

    const activeCard =
      activeView.querySelector(".offer-card.is-active") || activeView.querySelector(".offer-card");
    if (!activeCard) {
      return;
    }

    const viewName = activeView.dataset.view || "";
    const purchaseType =
      viewName === "premium" || viewName === "ton" ? viewName : "stars";
    const priceUsdText = activeView.querySelector("[data-price-usd]")?.textContent?.trim() || "";
    const priceRubText = activeView.querySelector("[data-price-rub]")?.textContent?.trim() || "";

    const formatRubHistory = (value) => {
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) {
        return "";
      }
      return `${Number.isInteger(numeric) ? numeric : numeric.toFixed(2)}₽`;
    };

    const formatUsdHistory = (value) => {
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) {
        return "";
      }
      return `${numeric.toFixed(2)}$`;
    };

    const cardAmountText = activeCard.querySelector(".offer-card-amount")?.textContent?.trim() || "";
    let amountText = cardAmountText;

    if (purchaseType === "stars") {
      const starsInput = activeView.querySelector('.form-input[type="number"]');
      const starsInputValue = String(starsInput?.value || "").trim();
      if (starsInputValue) {
        amountText = starsInputValue;
      }
    }

    if (override?.amount) {
      if (purchaseType === "premium") {
        amountText = `${override.amount} мес.`;
      } else {
        amountText = String(override.amount);
      }
    }

    let priceText = activeCard.querySelector(".offer-card-price")?.textContent?.trim() || "—";
    if (priceUsdText && priceRubText) {
      priceText = `${priceUsdText}/${priceRubText}`;
    }
    if (
      override &&
      override.priceUsd !== undefined &&
      override.priceRub !== undefined
    ) {
      const usd = formatUsdHistory(override.priceUsd);
      const rub = formatRubHistory(override.priceRub);
      priceText = usd && rub ? `${usd}/${rub}` : priceText;
    }

    historyItems.unshift({
      type: purchaseType,
      main: amountText || "—",
      price: priceText,
      createdAt: new Date().toISOString(),
    });
    historyItems = historyItems.slice(0, 20);

    saveHistory(historyItems);
    historyPage = 0;
    renderHistory();
  };

  const apiMeta = document.querySelector('meta[name="starslix-api-base"]');
  const apiBaseRaw = (apiMeta?.content || "").trim().replace(/\/+$/, "");
  const isMixedContentBlocked =
    window.location.protocol === "https:" && /^http:\/\//i.test(apiBaseRaw);
  const apiBase = isMixedContentBlocked ? "" : apiBaseRaw;
  const buildApiUrl = (path) => (apiBase ? `${apiBase}${path}` : path);
  const profileStorageKey = "starslix:profile-user";
  let currentUserIsAdmin = false;

  const showMiniappMessage = (text) => {
    if (window.Telegram?.WebApp?.showAlert) {
      window.Telegram.WebApp.showAlert(text);
      return;
    }
    window.alert(text);
  };

  const setAdminPanelVisibility = (isAdmin) => {
    currentUserIsAdmin = Boolean(isAdmin);
    if (profileAdminActions) {
      profileAdminActions.hidden = !currentUserIsAdmin;
    }
    if (profileAdminButton) {
      profileAdminButton.disabled = !currentUserIsAdmin;
      profileAdminButton.setAttribute("aria-hidden", currentUserIsAdmin ? "false" : "true");
    }
  };

  const parseUserFromInitData = (initDataRaw) => {
    if (!initDataRaw) {
      return null;
    }
    try {
      const params = new URLSearchParams(initDataRaw);
      const rawUser = params.get("user");
      if (!rawUser) {
        return null;
      }
      return JSON.parse(rawUser);
    } catch {
      return null;
    }
  };

  const normalizeProfileUser = (rawUser) => {
    if (!rawUser || typeof rawUser !== "object") {
      return null;
    }
    const firstName = String(rawUser.first_name ?? rawUser.firstName ?? "").trim();
    const lastName = String(rawUser.last_name ?? rawUser.lastName ?? "").trim();
    const usernameRaw = String(rawUser.username ?? "").trim().replace(/^@+/, "");
    const username = usernameRaw ? `@${usernameRaw}` : "";
    const telegramId = rawUser.id ? String(rawUser.id) : "";
    const fullName = String(rawUser.fullName ?? "").trim() || [firstName, lastName].filter(Boolean).join(" ").trim();
    const photoUrl = String(rawUser.photo_url ?? rawUser.photoUrl ?? "").trim();
    return {
      firstName,
      lastName,
      fullName,
      username,
      telegramId,
      photoUrl,
    };
  };

  const loadStoredProfileUser = () => {
    try {
      const raw = window.localStorage.getItem(profileStorageKey);
      return raw ? JSON.parse(raw) : null;
    } catch {
      return null;
    }
  };

  const saveStoredProfileUser = (profileUser) => {
    if (!profileUser) {
      return;
    }
    try {
      window.localStorage.setItem(profileStorageKey, JSON.stringify(profileUser));
    } catch {
      // ignore storage write errors
    }
  };

  const mergeProfileUsers = (baseUser, nextUser) => {
    const base = baseUser || {};
    const next = nextUser || {};
    return {
      firstName: next.firstName || base.firstName || "",
      lastName: next.lastName || base.lastName || "",
      fullName: next.fullName || base.fullName || "",
      username: next.username || base.username || "",
      telegramId: next.telegramId || base.telegramId || "",
      photoUrl: next.photoUrl || base.photoUrl || "",
    };
  };

  const applyProfileFields = (profileUser) => {
    const user = profileUser || {};
    if (profileUsername) {
      profileUsername.textContent = user.username || "—";
    }
    if (profileTelegramId) {
      profileTelegramId.textContent = user.telegramId || "—";
    }
    if (profileFullname) {
      const fullName = user.fullName || [user.firstName, user.lastName].filter(Boolean).join(" ").trim();
      if (fullName) {
        profileFullname.textContent = fullName;
        profileFullname.dataset.empty = "false";
      }
    }
  };

  const webAppUser = normalizeProfileUser(
    window.Telegram?.WebApp?.initDataUnsafe?.user ||
      parseUserFromInitData(window.Telegram?.WebApp?.initData || "")
  );
  const storedProfileUser = normalizeProfileUser(loadStoredProfileUser());
  let currentProfileUser = mergeProfileUsers(storedProfileUser, webAppUser);

  applyProfileFields(currentProfileUser);
  setAdminPanelVisibility(false);
  if (webAppUser) {
    saveStoredProfileUser(currentProfileUser);
  }

  const applyAvatarSrc = (src) => {
    appAvatarNodes.forEach((avatarNode) => {
      if (!(avatarNode instanceof HTMLImageElement)) {
        return;
      }
      const fallbackSrc =
        avatarNode.dataset.fallbackSrc ||
        avatarNode.getAttribute("src") ||
        "avauser.png";
      avatarNode.dataset.fallbackSrc = fallbackSrc;
      avatarNode.onerror = () => {
        if (avatarNode.getAttribute("src") !== fallbackSrc) {
          avatarNode.src = fallbackSrc;
        }
      };
      avatarNode.src = src || fallbackSrc;
    });
  };

  const avatarStorageKey = "starslix:avatar-url";
  let storedAvatarUrl = "";
  try {
    storedAvatarUrl = window.localStorage.getItem(avatarStorageKey) || "";
  } catch (error) {}

  const currentAvatarUrl =
    typeof currentProfileUser?.photoUrl === "string" ? currentProfileUser.photoUrl.trim() : "";

  if (currentAvatarUrl) {
    try {
      window.localStorage.setItem(avatarStorageKey, currentAvatarUrl);
    } catch (error) {}
  }

  applyAvatarSrc(currentAvatarUrl || storedAvatarUrl || "");

  const applyHistoryFromApi = (rawHistory) => {
    if (!Array.isArray(rawHistory)) {
      return;
    }
    historyItems = rawHistory
      .map((item) => ({
        type: normalizeHistoryType(item),
        main: String(item?.main || "").trim() || "—",
        price: String(item?.price || "").trim() || "—",
        createdAt: item?.createdAt || new Date().toISOString(),
      }))
      .slice(0, 20);
    saveHistory(historyItems);
    historyPage = 0;
    renderHistory();
  };

  const fetchProfileFromApi = async () => {
    const initData = window.Telegram?.WebApp?.initData || "";
    if (!initData) {
      return;
    }

    try {
      const response = await fetch(buildApiUrl("/api/miniapp/profile"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify({ initData }),
      });

      if (!response.ok) {
        return;
      }

      const data = await response.json().catch(() => null);
      if (!data?.ok) {
        return;
      }

      setAdminPanelVisibility(Boolean(data.isAdmin));

      const apiUser = normalizeProfileUser(data.user || {});
      if (apiUser) {
        currentProfileUser = mergeProfileUsers(currentProfileUser, apiUser);
        applyProfileFields(currentProfileUser);
        saveStoredProfileUser(currentProfileUser);

        if (currentProfileUser.photoUrl) {
          try {
            window.localStorage.setItem(avatarStorageKey, currentProfileUser.photoUrl);
          } catch {
            // ignore storage write errors
          }
          applyAvatarSrc(currentProfileUser.photoUrl);
        }
      }

      applyHistoryFromApi(data.history);
    } catch {
      // silent fallback to cached local profile/history
    }
  };

  if (avatarToggle) {
    avatarToggle.removeAttribute("disabled");
    avatarToggle.removeAttribute("aria-disabled");
    avatarToggle.type = "button";
    avatarToggle.setAttribute("aria-expanded", "false");
  }
  if (quickMenu) {
    quickMenu.classList.add("is-open");
    quickMenu.setAttribute("aria-hidden", "false");
  }

  avatarToggle?.addEventListener("click", (event) => {
    event.preventDefault();
    window.dispatchEvent(
      new CustomEvent("starslix:navigate", {
        detail: { screen: "profile" },
      })
    );
  });

  profileCloseButton?.addEventListener("click", (event) => {
    event.preventDefault();
    window.dispatchEvent(
      new CustomEvent("starslix:navigate", {
        detail: { screen: "home" },
      })
    );
  });

  profileHistoryPrev?.addEventListener("click", (event) => {
    event.preventDefault();
    if (historyPage <= 0) {
      return;
    }
    historyPage -= 1;
    renderHistory();
  });

  profileHistoryNext?.addEventListener("click", (event) => {
    event.preventDefault();
    const totalPages = Math.max(1, Math.ceil(historyItems.length / historyPageSize));
    if (historyPage >= totalPages - 1) {
      return;
    }
    historyPage += 1;
    renderHistory();
  });

  profileAdminButton?.addEventListener("click", (event) => {
    event.preventDefault();
    if (!currentUserIsAdmin) {
      return;
    }
    window.dispatchEvent(
      new CustomEvent("starslix:navigate", {
        detail: { screen: "admin" },
      })
    );
  });

  // Экспортируем, чтобы добавлять запись после успешной отправки заказа на backend.
  window.starslixAddCurrentOrderToHistory = addCurrentOrderToHistory;

  setProfilePageState(false);
  renderHistory();
  fetchProfileFromApi();
})();

(() => {
  const telegramBotButton = document.getElementById("telegramBotButton");
  if (!telegramBotButton) {
    return;
  }

  const telegramBotUrl = "https://t.me/starslixbot";

  telegramBotButton.addEventListener("click", (event) => {
    event.preventDefault();

    if (window.Telegram?.WebApp?.openTelegramLink) {
      window.Telegram.WebApp.openTelegramLink(telegramBotUrl);
      return;
    }

    window.open(telegramBotUrl, "_blank", "noopener,noreferrer");
  });
})();

(() => {
  const apiMeta = document.querySelector('meta[name="starslix-api-base"]');
  const apiBaseRaw = (apiMeta?.content || "").trim().replace(/\/+$/, "");
  const isMixedContentBlocked =
    window.location.protocol === "https:" && /^http:\/\//i.test(apiBaseRaw);
  const apiBase = isMixedContentBlocked ? "" : apiBaseRaw;
  const buildApiUrl = (path) => (apiBase ? `${apiBase}${path}` : path);

  const showMessage = (text) => {
    if (window.Telegram?.WebApp?.showAlert) {
      window.Telegram.WebApp.showAlert(text);
      return;
    }
    window.alert(text);
  };

  const formatRub = (value) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return "—";
    }
    return `${Number.isInteger(numeric) ? numeric : numeric.toFixed(2)}₽`;
  };

  const formatUsd = (value) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return "—";
    }
    return `${numeric.toFixed(2)}$`;
  };

  const syncPanelPriceRow = (gridNode) => {
    const panelView = gridNode?.closest(".panel-view");
    if (!panelView) {
      return;
    }
    const priceUsdNode = panelView.querySelector("[data-price-usd]");
    const priceRubNode = panelView.querySelector("[data-price-rub]");
    if (!priceUsdNode || !priceRubNode) {
      return;
    }

    const sourceCard =
      gridNode.querySelector(".offer-card.is-active");
    if (!sourceCard) {
      priceUsdNode.textContent = "—";
      priceRubNode.textContent = "—";
      return;
    }

    const rawPrice = sourceCard.querySelector(".offer-card-price")?.textContent?.trim() || "";
    const [usdPart = "—", rubPart = "—"] = rawPrice.split("/");
    priceUsdNode.textContent = usdPart.trim() || "—";
    priceRubNode.textContent = rubPart.trim() || "—";
  };

  const applyConfigToCards = (config) => {
    window.starslixMiniappConfig = {
      ...(window.starslixMiniappConfig || {}),
      ...(config || {}),
    };

    const starsCards = Array.from(document.querySelectorAll(".offer-grid-stars .offer-card"));
    (config?.stars || []).slice(0, starsCards.length).forEach((offer, index) => {
      const card = starsCards[index];
      const amountNode = card.querySelector(".offer-card-amount");
      const priceNode = card.querySelector(".offer-card-price");
      if (amountNode) {
        amountNode.textContent = String(offer.amount);
      }
      if (priceNode) {
        priceNode.textContent = `${formatUsd(offer.usd)}/${formatRub(offer.rub)}`;
      }
      card.dataset.amountValue = String(offer.amount);
    });

    const premiumCards = Array.from(document.querySelectorAll(".offer-grid-premium .offer-card"));
    (config?.premium || []).slice(0, premiumCards.length).forEach((offer, index) => {
      const card = premiumCards[index];
      const priceNode = card.querySelector(".offer-card-price");
      if (priceNode) {
        priceNode.textContent = `${formatUsd(offer.usd)}/${formatRub(offer.rub)}`;
      }
      card.dataset.amountValue = String(offer.months);
    });

    document.querySelectorAll(".offer-grid").forEach(syncPanelPriceRow);
    window.dispatchEvent(new CustomEvent("starslix:pricing-updated"));
  };

  const fetchMiniappConfig = async () => {
    try {
      const response = await fetch(buildApiUrl("/api/miniapp/config"), {
        method: "GET",
        headers: {
          Accept: "application/json",
        },
      });
      if (!response.ok) {
        return;
      }

      const data = await response.json();
      if (data?.ok) {
        applyConfigToCards(data);
      }
    } catch (error) {}
  };

  const clearPanelInputs = (panelView) => {
    if (!panelView) {
      return;
    }

    panelView.querySelectorAll(".form-input").forEach((inputNode) => {
      if (!(inputNode instanceof HTMLInputElement || inputNode instanceof HTMLTextAreaElement)) {
        return;
      }
      inputNode.value = "";
      inputNode.dispatchEvent(new Event("input", { bubbles: true }));
      inputNode.dispatchEvent(new Event("change", { bubbles: true }));
    });

    const activeElement = document.activeElement;
    if (activeElement instanceof HTMLElement && panelView.contains(activeElement)) {
      activeElement.blur();
    }
  };

  const parseAmountFromCard = (card) => {
    if (!card) {
      return "";
    }
    if (card.dataset.amountValue) {
      return card.dataset.amountValue;
    }
    const amountText = card.querySelector(".offer-card-amount")?.textContent?.trim() || "";
    return amountText.replace(/[^0-9.]/g, "");
  };

  const getOrderPayload = (panelView) => {
    const type = panelView?.dataset?.view;
    if (type !== "stars" && type !== "premium") {
      return null;
    }

    const usernameInput = panelView.querySelector(".form-input--telegram");
    const promoInput = panelView.querySelector(".form-input--promo");
    const username = usernameInput?.value?.trim() || "";
    const promoCode = promoInput?.value?.trim() || "";

    let amount = "";
    if (type === "stars") {
      const amountInput = panelView.querySelector('.form-input[type="number"]');
      const selectedCard =
        panelView.querySelector(".offer-card.is-active") ||
        panelView.querySelector(".offer-grid-stars .offer-card");
      amount = (amountInput?.value || "").trim() || parseAmountFromCard(selectedCard);
    } else {
      const selectedCard =
        panelView.querySelector(".offer-card.is-active") ||
        panelView.querySelector(".offer-grid-premium .offer-card");
      amount = parseAmountFromCard(selectedCard);
    }

    if (!username) {
      return { error: "Введите Telegram username" };
    }
    if (!amount) {
      return { error: type === "stars" ? "Введите количество звёзд" : "Выберите пакет Premium" };
    }

    const initData = window.Telegram?.WebApp?.initData || "";
    const hasSendData = typeof window.Telegram?.WebApp?.sendData === "function";
    // Even with empty meta base we can call same-origin `/api/miniapp/...`.
    const shouldUseApi = true;
    if (shouldUseApi && !initData) {
      return { error: "Открой мини-приложение через Telegram" };
    }
    if (!shouldUseApi && !initData && !hasSendData) {
      return { error: "Открой мини-приложение через Telegram" };
    }

    return {
      type,
      username,
      amount,
      promoCode,
      initData,
    };
  };

  const submitOrder = async (button, panelView) => {
    const payload = getOrderPayload(panelView);
    if (!payload || payload.error) {
      showMessage(payload?.error || "Некорректные данные заказа");
      return;
    }

    const parsePriceFromNode = (node) => {
      const rawValue = String(node?.textContent || "").trim();
      if (!rawValue || rawValue === "—") {
        return null;
      }
      const normalized = rawValue.replace(",", ".").replace(/[^0-9.\-]/g, "");
      const numeric = Number.parseFloat(normalized);
      return Number.isFinite(numeric) ? numeric : null;
    };

    const currentPriceUsd = parsePriceFromNode(panelView.querySelector("[data-price-usd]"));
    const currentPriceRub = parsePriceFromNode(panelView.querySelector("[data-price-rub]"));

    const trySendViaWebAppData = () => {
      if (!window.Telegram?.WebApp?.sendData) {
        return false;
      }

      window.Telegram.WebApp.sendData(
        JSON.stringify({
          source: "miniapp",
          type: payload.type,
          username: payload.username,
          amount: payload.amount,
          promoCode: payload.promoCode,
          clientPrice: {
            usd: currentPriceUsd,
            rub: currentPriceRub,
          },
        })
      );

      if (typeof window.starslixAddCurrentOrderToHistory === "function") {
        window.starslixAddCurrentOrderToHistory({
          type: payload.type,
          amount: payload.amount,
          priceUsd: currentPriceUsd,
          priceRub: currentPriceRub,
        });
      }

      clearPanelInputs(panelView);
      showMessage("Запрос отправлен в Telegram. Если бот не ответил в чате, открой Mini App через кнопку в чате бота или включи HTTPS API.");
      return true;
    };

    button.disabled = true;
    button.classList.add("is-loading");

    try {
      const response = await fetch(buildApiUrl("/api/miniapp/order"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify(payload),
      });
      const data = await response.json().catch(() => ({}));

      if (!response.ok || !data?.ok) {
        if (trySendViaWebAppData()) {
          return;
        }
        const details = data?.error || `Ошибка API (${response.status})`;
        showMessage(`Не удалось отправить заявку: ${details}`);
        return;
      }

      if (typeof window.starslixAddCurrentOrderToHistory === "function") {
        window.starslixAddCurrentOrderToHistory({
          type: payload.type,
          amount: payload.amount,
          priceUsd: data?.price?.usd,
          priceRub: data?.price?.rub,
        });
      }

      clearPanelInputs(panelView);
      const promoError = data?.promo?.error ? `\nПромокод: ${data.promo.error}` : "";
      showMessage(`Заявка отправлена в группу${promoError}`);
      return;
    } catch (error) {
      try {
        if (trySendViaWebAppData()) {
          return;
        }
      } catch (fallbackError) {}
      showMessage(
        `Ошибка сети при отправке заявки: ${error?.message || "unknown error"}`
      );
    } finally {
      button.disabled = false;
      button.classList.remove("is-loading");
    }
  };

  document
    .querySelectorAll(".form-stack--stars .order-submit-button, .form-stack--premium .order-submit-button")
    .forEach((button) => {
      button.addEventListener("click", (event) => {
        event.preventDefault();
        const panelView = button.closest(".panel-view");
        if (!panelView) {
          return;
        }
        submitOrder(button, panelView);
      });
    });

  document.querySelectorAll(".form-stack--ton .order-submit-button").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.preventDefault();
      const panelView = button.closest(".panel-view");
      clearPanelInputs(panelView);
    });
  });

  fetchMiniappConfig();
})();

(() => {
  const languageToggleButton = document.getElementById("languageToggleButton");
  const languageDropdown = document.getElementById("languageDropdown");
  const languageSwitch = document.getElementById("headerLanguageSwitch");
  const languageOptions = Array.from(document.querySelectorAll(".header-language-option"));
  if (!languageToggleButton || !languageDropdown || !languageSwitch || !languageOptions.length) {
    return;
  }

  const translations = {
    ru: {
      loaderAria: "Загрузка",
      profileButtonAria: "Открыть страницу профиля",
      telegramButton: "Telegram-бот",
      languageButton: "Смена языка",
      languageShort: "RU",
      languageOptionRu: "Русский",
      languageOptionEn: "Английский",
      heroMobileLine1: "Получите",
      heroMobileLine2Prefix: "свои ",
      heroStars: "звёзды",
      heroMobileLine3: "здесь!",
      heroWideLine1: "Получите свои",
      heroWideLine2Suffix: "здесь!",
      heroSubtitleLine1: "Покупайте звёзды быстро и анонимно.",
      heroSubtitleLine2: "Никаких KYC верификации и риска возврата.",
      topPillAria: "Улучшите свои впечатления от Telegram",
      topPillText: "Улучшите свои впечатления от Telegram",
      dockAria: "Нижнее меню",
      dockHome: "Главная",
      dockLeaders: "Лидеры",
      dockContest: "Конкурс",
      dockProfile: "Профиль",
      leadersPageAria: "Страница лидеров",
      leadersTitle: "Лидеры",
      leadersText: "Раздел лидеров в разработке.",
      contestPageAria: "Страница конкурса",
      contestTitle: "Конкурс",
      contestText: "Раздел конкурса в разработке.",
      mainPanelAria: "Основной блок контента",
      panelTabsAria: "Разделы",
      panelTabStarsAria: "Звёзды",
      panelTabPremiumAria: "Премиум",
      starsGridAria: "Пакеты звёзд",
      premiumGridAria: "Пакеты премиума",
      cardStars100Aria: "Пакет звёзд 100",
      cardStars500Aria: "Пакет звёзд 500",
      cardStars1000Aria: "Пакет звёзд 1000",
      cardPremium100Aria: "Пакет премиума 3 месяца",
      cardPremium500Aria: "Пакет премиума 6 месяцев",
      cardPremium1000Aria: "Пакет премиума 12 месяцев",
      premiumDuration3: "3 мес.",
      premiumDuration6: "6 мес.",
      premiumDuration12: "12 мес.",
      badgeValue: "Выгодно",
      badgePopular: "Популярно",
      usernameFloatingLabel: "Введите Telegram @username",
      selfFillButton: "Себе",
      amountFloatingLabel: "Введите количество (50 - 10000)",
      tonAmountFloatingLabel: "Введите количество TON",
      promoFloatingLabel: "Введите промокод",
      paymentMethodLabel: "Метод оплаты",
      paymentMethodText: "СПБ #1 (PALLY)",
      orderButton: "Оформить заказ",
      tonOrderButton: "Отправить TON",
      orderNote:
        "Приобретая Telegram Stars или Telegram Premium, вы подтверждаете свое согласие с нашими Условиями и подтверждаете, что не являетесь резидентом США.",
      profilePageAria: "Страница профиля",
      profileCloseAria: "Закрыть профиль",
      profileFallbackName: "Пользователь Telegram",
      profileLabelUsername: "Telegram username",
      profileLabelTelegramId: "Telegram ID",
      profileHistoryTitle: "История покупок",
      profileHistoryEmpty: "Покупок пока нет",
      profileAdminButton: "Админ-панель",
    },
    en: {
      loaderAria: "Loading",
      profileButtonAria: "Open profile page",
      telegramButton: "Telegram bot",
      languageButton: "Switch language",
      languageShort: "EN",
      languageOptionRu: "Russian",
      languageOptionEn: "English",
      heroMobileLine1: "Get",
      heroMobileLine2Prefix: "your ",
      heroStars: "stars",
      heroMobileLine3: "here!",
      heroWideLine1: "Get your",
      heroWideLine2Suffix: "here!",
      heroSubtitleLine1: "Buy stars quickly and anonymously.",
      heroSubtitleLine2: "No KYC verification and no chargeback risk.",
      topPillAria: "Enhance your Telegram experience",
      topPillText: "Enhance your Telegram experience",
      dockAria: "Bottom menu",
      dockHome: "Home",
      dockLeaders: "Leaders",
      dockContest: "Contest",
      dockProfile: "Profile",
      leadersPageAria: "Leaders page",
      leadersTitle: "Leaders",
      leadersText: "Leaders section is in progress.",
      contestPageAria: "Contest page",
      contestTitle: "Contest",
      contestText: "Contest section is in progress.",
      mainPanelAria: "Main content section",
      panelTabsAria: "Sections",
      panelTabStarsAria: "Stars",
      panelTabPremiumAria: "Premium",
      starsGridAria: "Star packages",
      premiumGridAria: "Premium packages",
      cardStars100Aria: "100 stars package",
      cardStars500Aria: "500 stars package",
      cardStars1000Aria: "1000 stars package",
      cardPremium100Aria: "Premium package 3 months",
      cardPremium500Aria: "Premium package 6 months",
      cardPremium1000Aria: "Premium package 12 months",
      premiumDuration3: "3 mo.",
      premiumDuration6: "6 mo.",
      premiumDuration12: "12 mo.",
      badgeValue: "Best value",
      badgePopular: "Popular",
      usernameFloatingLabel: "Enter Telegram @username",
      selfFillButton: "Me",
      amountFloatingLabel: "Enter amount (50 - 10000)",
      tonAmountFloatingLabel: "Enter TON amount",
      promoFloatingLabel: "Enter promo code",
      paymentMethodLabel: "Payment method",
      paymentMethodText: "SBP #1 (PALLY)",
      orderButton: "Place order",
      tonOrderButton: "Send TON",
      orderNote:
        "By purchasing Telegram Stars or Telegram Premium, you confirm your agreement with our Terms and confirm that you are not a resident of the USA.",
      profilePageAria: "Profile page",
      profileCloseAria: "Close profile",
      profileFallbackName: "Telegram user",
      profileLabelUsername: "Telegram username",
      profileLabelTelegramId: "Telegram ID",
      profileHistoryTitle: "Purchase history",
      profileHistoryEmpty: "No purchases yet",
      profileAdminButton: "Admin panel",
    },
  };

  const storageKey = "starslix_lang";

  const setText = (selector, value) => {
    const element = document.querySelector(selector);
    if (element) {
      element.textContent = value;
    }
  };

  const setAttr = (selector, attr, value) => {
    const element = document.querySelector(selector);
    if (element) {
      element.setAttribute(attr, value);
    }
  };

  const setHeroSecondLines = (dict) => {
    const mobileLine2 = document.querySelector(
      ".hero-title-layout-mobile .hero-title-line:nth-child(2)"
    );
    if (mobileLine2) {
      mobileLine2.innerHTML = `${dict.heroMobileLine2Prefix}<span class="hero-title-accent">${dict.heroStars}</span>`;
    }

    const wideLine2 = document.querySelector(
      ".hero-title-layout-wide .hero-title-line:nth-child(2)"
    );
    if (wideLine2) {
      wideLine2.innerHTML = `<span class="hero-title-accent">${dict.heroStars}</span> ${dict.heroWideLine2Suffix}`;
    }
  };

  const applyLanguage = (lang) => {
    const dict = translations[lang] || translations.ru;

    document.documentElement.lang = lang;
    setAttr("#pageLoader", "aria-label", dict.loaderAria);
    setAttr("#userAvatarToggle", "aria-label", dict.profileButtonAria);

    setText("#telegramBotButton", dict.telegramButton);
    setAttr("#languageToggleButton", "aria-label", dict.languageButton);
    setText("#languageToggleValue", dict.languageShort);
    setText("#languageOptionRu", dict.languageOptionRu);
    setText("#languageOptionEn", dict.languageOptionEn);

    languageOptions.forEach((option) => {
      option.classList.toggle("is-active", option.dataset.lang === lang);
    });

    setText(".hero-title-layout-mobile .hero-title-line:nth-child(1)", dict.heroMobileLine1);
    setHeroSecondLines(dict);
    setText(".hero-title-layout-mobile .hero-title-line:nth-child(3)", dict.heroMobileLine3);
    setText(".hero-title-layout-wide .hero-title-line:nth-child(1)", dict.heroWideLine1);

    setText(".hero-subtitle-line:nth-child(1)", dict.heroSubtitleLine1);
    setText(".hero-subtitle-line:nth-child(2)", dict.heroSubtitleLine2);
    setAttr("#topPill", "aria-label", dict.topPillAria);
    setText("#topPillText", dict.topPillText);
    setAttr("#appDock", "aria-label", dict.dockAria);
    setText("#dockLabelHome", dict.dockHome);
    setText('[data-dock-label-tint="home"]', dict.dockHome);
    setText("#dockLabelLeaders", dict.dockLeaders);
    setText('[data-dock-label-tint="leaders"]', dict.dockLeaders);
    setText("#dockLabelContest", dict.dockContest);
    setText('[data-dock-label-tint="contest"]', dict.dockContest);
    setText("#dockLabelProfile", dict.dockProfile);
    setText('[data-dock-label-tint="profile"]', dict.dockProfile);
    setAttr("#leadersPage", "aria-label", dict.leadersPageAria);
    setText("#leadersTitle", dict.leadersTitle);
    setText("#leadersText", dict.leadersText);
    setAttr("#contestPage", "aria-label", dict.contestPageAria);
    setText("#contestTitle", dict.contestTitle);
    setText("#contestText", dict.contestText);

    setAttr(".main-glass-panel", "aria-label", dict.mainPanelAria);
    setAttr(".panel-tabs", "aria-label", dict.panelTabsAria);
    setAttr('.panel-tab[data-panel="stars"]', "aria-label", dict.panelTabStarsAria);
    setAttr('.panel-tab[data-panel="premium"]', "aria-label", dict.panelTabPremiumAria);

    setAttr(".offer-grid-stars", "aria-label", dict.starsGridAria);
    setAttr(".offer-grid-premium", "aria-label", dict.premiumGridAria);
    setAttr(".offer-grid-stars .offer-card:nth-child(1)", "aria-label", dict.cardStars100Aria);
    setAttr(".offer-grid-stars .offer-card:nth-child(2)", "aria-label", dict.cardStars500Aria);
    setAttr(".offer-grid-stars .offer-card:nth-child(3)", "aria-label", dict.cardStars1000Aria);
    setAttr(".offer-grid-premium .offer-card:nth-child(1)", "aria-label", dict.cardPremium100Aria);
    setAttr(".offer-grid-premium .offer-card:nth-child(2)", "aria-label", dict.cardPremium500Aria);
    setAttr(".offer-grid-premium .offer-card:nth-child(3)", "aria-label", dict.cardPremium1000Aria);
    setText(".offer-grid-premium .offer-card:nth-child(1) .offer-card-amount", dict.premiumDuration3);
    setText(".offer-grid-premium .offer-card:nth-child(2) .offer-card-amount", dict.premiumDuration6);
    setText(".offer-grid-premium .offer-card:nth-child(3) .offer-card-amount", dict.premiumDuration12);

    document.querySelectorAll(".offer-card--value .offer-card-badge").forEach((badge) => {
      badge.textContent = dict.badgeValue;
    });
    document.querySelectorAll(".offer-card--popular .offer-card-badge").forEach((badge) => {
      badge.textContent = dict.badgePopular;
    });

    setText(
      ".form-stack--stars .form-field:nth-child(1) .form-floating-label",
      dict.usernameFloatingLabel
    );
    setText(
      ".form-stack--stars .form-field:nth-child(2) .form-floating-label",
      dict.amountFloatingLabel
    );
    setText(
      ".form-stack--stars .form-field:nth-child(3) .form-floating-label",
      dict.promoFloatingLabel
    );
    setText(
      ".form-stack--stars .form-field:nth-child(4) .form-field-label",
      dict.paymentMethodLabel
    );
    setText(
      ".form-stack--premium .form-field:nth-child(1) .form-floating-label",
      dict.usernameFloatingLabel
    );
    setText(
      ".form-stack--premium .form-field:nth-child(2) .form-floating-label",
      dict.promoFloatingLabel
    );
    setText(
      ".form-stack--premium .form-field:nth-child(3) .form-field-label",
      dict.paymentMethodLabel
    );
    setText(
      ".form-stack--ton .form-field:nth-child(1) .form-floating-label",
      dict.usernameFloatingLabel
    );
    setText(
      ".form-stack--ton .form-field:nth-child(2) .form-floating-label",
      dict.tonAmountFloatingLabel
    );
    setText(
      ".form-stack--ton .form-field:nth-child(3) .form-field-label",
      dict.paymentMethodLabel
    );

    setAttr("#profilePage", "aria-label", dict.profilePageAria);
    setAttr("#profileCloseButton", "aria-label", dict.profileCloseAria);
    setText("#profileLabelUsername", dict.profileLabelUsername);
    setText("#profileLabelId", dict.profileLabelTelegramId);
    setText("#profileHistoryTitle", dict.profileHistoryTitle);
    setText("#profileHistoryEmpty", dict.profileHistoryEmpty);
    setText("#profileAdminButton", dict.profileAdminButton);
    document.querySelectorAll(".form-self-fill-button").forEach((node) => {
      node.textContent = dict.selfFillButton;
    });

    const profileFullname = document.getElementById("profileFullname");
    if (profileFullname?.dataset.empty === "true") {
      profileFullname.textContent = dict.profileFallbackName;
    }

    document.querySelectorAll(".form-static-text").forEach((node) => {
      node.textContent = dict.paymentMethodText;
    });
    document
      .querySelectorAll(".form-stack--stars .order-submit-button, .form-stack--premium .order-submit-button")
      .forEach((node) => {
      node.textContent = dict.orderButton;
    });
    setText(".form-stack--ton .order-submit-button", dict.tonOrderButton);
    document.querySelectorAll(".order-note").forEach((node) => {
      node.textContent = dict.orderNote;
    });

    window.dispatchEvent(new CustomEvent("starslix:dock-labels-updated"));
  };

  const getSavedLanguage = () => {
    try {
      const savedLanguage = window.localStorage.getItem(storageKey);
      return savedLanguage === "en" || savedLanguage === "ru" ? savedLanguage : null;
    } catch {
      return null;
    }
  };

  const saveLanguage = (lang) => {
    try {
      window.localStorage.setItem(storageKey, lang);
    } catch {
      // ignore write errors
    }
  };

  let currentLanguage = getSavedLanguage() || "ru";
  applyLanguage(currentLanguage);

  let isDropdownOpen = false;

  const setDropdownState = (open) => {
    isDropdownOpen = open;
    languageDropdown.classList.toggle("is-open", open);
    languageDropdown.setAttribute("aria-hidden", open ? "false" : "true");
    languageToggleButton.setAttribute("aria-expanded", open ? "true" : "false");
  };

  languageToggleButton.addEventListener("click", (event) => {
    event.stopPropagation();
    setDropdownState(!isDropdownOpen);
  });

  languageOptions.forEach((option) => {
    option.addEventListener("click", (event) => {
      event.stopPropagation();
      const nextLanguage = option.dataset.lang;
      if (nextLanguage !== "ru" && nextLanguage !== "en") {
        return;
      }

      currentLanguage = nextLanguage;
      applyLanguage(currentLanguage);
      saveLanguage(currentLanguage);
      setDropdownState(false);
    });
  });

  document.addEventListener("pointerdown", (event) => {
    if (!isDropdownOpen) {
      return;
    }

    const target = event.target;
    if (!(target instanceof Element)) {
      return;
    }

    if (target.closest("#headerLanguageSwitch")) {
      return;
    }

    setDropdownState(false);
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && isDropdownOpen) {
      setDropdownState(false);
    }
  });
})();

(() => {
  const adminHubPage = document.getElementById("adminHubPage");
  const adminSectionPage = document.getElementById("adminSectionPage");
  const adminHubCloseButton = document.getElementById("adminHubCloseButton");
  const adminSectionCloseButton = document.getElementById("adminSectionCloseButton");
  const adminSectionContent = document.getElementById("adminSectionContent");
  const adminSectionTitle = document.getElementById("adminSectionTitle");
  const adminSectionSubtitle = document.getElementById("adminSectionSubtitle");
  const sectionButtons = Array.from(document.querySelectorAll("[data-admin-section]"));
  if (
    !adminHubPage ||
    !adminSectionPage ||
    !adminSectionContent ||
    !adminSectionTitle ||
    !adminSectionSubtitle ||
    !sectionButtons.length
  ) {
    return;
  }

  const apiMeta = document.querySelector('meta[name="starslix-api-base"]');
  const apiBaseRaw = (apiMeta?.content || "").trim().replace(/\/+$/, "");
  const isMixedContentBlocked =
    window.location.protocol === "https:" && /^http:\/\//i.test(apiBaseRaw);
  const apiBase = isMixedContentBlocked ? "" : apiBaseRaw;
  const buildApiUrl = (path) => (apiBase ? `${apiBase}${path}` : path);

  const getInitData = () => String(window.Telegram?.WebApp?.initData || "").trim();
  const navigateToScreen = (screen) => {
    window.dispatchEvent(
      new CustomEvent("starslix:navigate", {
        detail: { screen },
      })
    );
  };

  const escapeHtml = (value) =>
    String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");

  const formatNumber = (value) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return "0";
    }
    return numeric.toLocaleString("ru-RU");
  };

  const formatMoney = (value) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return "0.00";
    }
    return numeric.toFixed(2);
  };

  const toIsoDate = (date) => {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, "0");
    const day = String(date.getDate()).padStart(2, "0");
    return `${year}-${month}-${day}`;
  };

  const stripDataUrlPrefix = (value) => {
    const text = String(value || "");
    const commaIndex = text.indexOf(",");
    return commaIndex >= 0 ? text.slice(commaIndex + 1) : text;
  };

  const fileToDataUrl = (file) =>
    new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(String(reader.result || ""));
      reader.onerror = () => reject(new Error("Не удалось прочитать файл"));
      reader.readAsDataURL(file);
    });

  const parseButtonsText = (rawValue) => {
    const lines = String(rawValue || "")
      .split(/\r?\n/g)
      .map((line) => line.trim())
      .filter(Boolean);

    const buttons = [];
    for (const line of lines) {
      const separatorIndex = line.indexOf(" - ");
      if (separatorIndex <= 0) {
        throw new Error("Формат кнопок: Текст - https://ссылка");
      }
      const text = line.slice(0, separatorIndex).trim();
      const url = line.slice(separatorIndex + 3).trim();
      if (!text || !url || !/^(https?:\/\/|tg:\/\/)/i.test(url)) {
        throw new Error("Проверьте кнопки: ссылка должна начинаться с http://, https:// или tg://");
      }
      buttons.push({ text, url });
    }
    return buttons;
  };

  const showAlert = (text) => {
    if (window.Telegram?.WebApp?.showAlert) {
      window.Telegram.WebApp.showAlert(text);
      return;
    }
    window.alert(text);
  };

  const adminRequest = async (action, payload = {}) => {
    const initData = getInitData();
    if (!initData) {
      throw new Error("Открой мини-приложение через Telegram");
    }

    const response = await fetch(buildApiUrl("/api/miniapp/admin/action"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: JSON.stringify({
        initData,
        action,
        payload,
      }),
    });

    const data = await response.json().catch(() => ({}));
    if (!response.ok || !data?.ok) {
      throw new Error(data?.error || "Не удалось выполнить действие");
    }
    return data?.data || {};
  };

  const requestAdminRates = async (starRates = null) => {
    const initData = getInitData();
    if (!initData) {
      throw new Error("Открой мини-приложение через Telegram");
    }

    const body = { initData };
    if (starRates) {
      body.starRates = starRates;
    }

    const response = await fetch(buildApiUrl("/api/miniapp/admin/rates"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: JSON.stringify(body),
    });

    const data = await response.json().catch(() => ({}));
    if (!response.ok || !data?.ok) {
      throw new Error(data?.error || "Не удалось получить тарифы");
    }
    return data;
  };

  const getSectionTitleByKey = (sectionKey) =>
    sectionButtons.find((button) => button.dataset.adminSection === sectionKey)?.textContent?.trim() ||
    "Раздел";

  const sectionSubtitles = {
    stats: "Пользователи, продажи и очистка",
    promos: "Создание, активные и удаление",
    referrals: "Показ и сброс активных рефералов",
    stars: "Проданные звёзды и тарифы",
    payment: "Баланс, история и выплаты",
    broadcast: "Текст, фото и кнопки ссылок",
    "find-user": "Поиск по username или ID",
    "add-stars": "Ручное начисление звёзд",
    admins: "Добавление и удаление админов",
  };

  // Section renderers are defined below.
  let sectionRenderers = {};

  const openSection = async (sectionKey) => {
    adminSectionTitle.textContent = getSectionTitleByKey(sectionKey);
    adminSectionSubtitle.textContent = sectionSubtitles[sectionKey] || "Выберите действие";
    adminSectionContent.innerHTML = "";
    navigateToScreen("admin-section");

    const renderer = sectionRenderers[sectionKey];
    if (typeof renderer === "function") {
      await renderer();
      return;
    }
    adminSectionContent.innerHTML = '<div class="admin-result-card">Раздел в разработке.</div>';
  };

  const renderStatsSection = async () => {
    adminSectionContent.innerHTML = `
      <div class="admin-panel-actions">
        <button class="admin-action-button" id="adminStatsUsersButton" type="button">Пользователи</button>
        <button class="admin-action-button" id="adminStatsSalesButton" type="button">Продажи</button>
        <button class="admin-action-button" id="adminStatsClearButton" type="button">Очистить статистику</button>
      </div>
      <div class="admin-split" id="adminStatsUsersSearchRow" hidden>
        <input class="admin-input" id="adminStatsUsersSearchInput" type="text" placeholder="Поиск по тг юзу или ID">
        <button class="admin-action-button" id="adminStatsUsersSearchButton" type="button">Найти</button>
      </div>
      <div class="admin-table-wrap" id="adminStatsUsersTableWrap" hidden>
        <table class="admin-table admin-table--compact">
          <thead><tr><th>Тг юз</th><th>Тг ID</th></tr></thead>
          <tbody id="adminStatsUsersTableBody"><tr><td colspan="2">—</td></tr></tbody>
        </table>
      </div>
      <div class="admin-pagination" id="adminStatsUsersPagination" hidden>
        <button class="admin-action-button" id="adminStatsUsersPrevButton" type="button">← Назад</button>
        <div class="admin-page-indicator" id="adminStatsUsersPageInfo">Страница 1 / 1</div>
        <button class="admin-action-button" id="adminStatsUsersNextButton" type="button">Вперёд →</button>
      </div>
      <div class="admin-result-card" id="adminStatsResult" hidden></div>
    `;

    const usersButton = document.getElementById("adminStatsUsersButton");
    const salesButton = document.getElementById("adminStatsSalesButton");
    const clearButton = document.getElementById("adminStatsClearButton");
    const usersSearchRow = document.getElementById("adminStatsUsersSearchRow");
    const usersSearchInput = document.getElementById("adminStatsUsersSearchInput");
    const usersSearchButton = document.getElementById("adminStatsUsersSearchButton");
    const usersTableWrap = document.getElementById("adminStatsUsersTableWrap");
    const usersTableBody = document.getElementById("adminStatsUsersTableBody");
    const usersPagination = document.getElementById("adminStatsUsersPagination");
    const usersPrevButton = document.getElementById("adminStatsUsersPrevButton");
    const usersNextButton = document.getElementById("adminStatsUsersNextButton");
    const usersPageInfo = document.getElementById("adminStatsUsersPageInfo");
    const resultNode = document.getElementById("adminStatsResult");
    if (
      !usersButton ||
      !salesButton ||
      !clearButton ||
      !usersSearchRow ||
      !usersSearchInput ||
      !usersSearchButton ||
      !usersTableWrap ||
      !usersTableBody ||
      !usersPagination ||
      !usersPrevButton ||
      !usersNextButton ||
      !usersPageInfo ||
      !resultNode
    ) {
      return;
    }

    const setButtonsState = (disabled) => {
      usersButton.disabled = disabled;
      salesButton.disabled = disabled;
      clearButton.disabled = disabled;
    };

    let usersPage = 1;
    let usersTotalPages = 1;
    let usersQuery = "";
    let usersLoading = false;

    const showUsersListUi = () => {
      usersSearchRow.hidden = false;
      usersTableWrap.hidden = false;
      usersPagination.hidden = false;
    };

    const updateUsersPaginationState = () => {
      usersPageInfo.textContent = `Страница ${usersPage} / ${usersTotalPages}`;
      usersPrevButton.disabled = usersLoading || usersPage <= 1;
      usersNextButton.disabled = usersLoading || usersPage >= usersTotalPages;
    };

    const setUsersLoadingState = (disabled) => {
      usersLoading = disabled;
      usersSearchInput.disabled = disabled;
      usersSearchButton.disabled = disabled;
      usersButton.disabled = disabled;
      updateUsersPaginationState();
    };

    const setStatsResult = (text = "") => {
      const safeText = String(text || "");
      resultNode.textContent = safeText;
      resultNode.hidden = !safeText;
    };

    const renderUsersRows = (items) => {
      if (!items.length) {
        usersTableBody.innerHTML = '<tr><td colspan="2">Пользователи не найдены</td></tr>';
        return;
      }

      usersTableBody.innerHTML = items
        .map((item) => {
          const usernameRaw = String(item?.username || "").trim().replace(/^@+/, "");
          const usernameText = usernameRaw ? `@${escapeHtml(usernameRaw)}` : "—";
          const userIdText = escapeHtml(String(item?.id ?? "—"));
          return `<tr><td>${usernameText}</td><td>${userIdText}</td></tr>`;
        })
        .join("");
    };

    const loadUsersPage = async ({ page, query }) => {
      showUsersListUi();
      usersTableBody.innerHTML = '<tr><td colspan="2">Загрузка...</td></tr>';
      setUsersLoadingState(true);

      try {
        const data = await adminRequest("stats_users_list", { page, query, limit: 8 });
        const items = Array.isArray(data.items) ? data.items : [];
        const safePage = Number.parseInt(data.page, 10);
        const safeTotalPages = Number.parseInt(data.totalPages, 10);
        const safeTotal = Number.parseInt(data.total, 10);

        usersPage = Number.isFinite(safePage) && safePage > 0 ? safePage : 1;
        usersTotalPages = Number.isFinite(safeTotalPages) && safeTotalPages > 0 ? safeTotalPages : 1;
        usersQuery = String(data.query ?? query ?? "").trim();

        renderUsersRows(items);
        setStatsResult(`Пользователей: ${formatNumber(Number.isFinite(safeTotal) ? safeTotal : 0)}`);
      } catch (error) {
        usersTableBody.innerHTML = `<tr><td colspan="2">${escapeHtml(error.message)}</td></tr>`;
        usersPage = 1;
        usersTotalPages = 1;
        setStatsResult(error.message);
      } finally {
        setUsersLoadingState(false);
      }
    };

    usersButton.addEventListener("click", async () => {
      usersQuery = usersSearchInput.value.trim();
      usersPage = 1;
      await loadUsersPage({ page: usersPage, query: usersQuery });
    });

    salesButton.addEventListener("click", async () => {
      setButtonsState(true);
      try {
        const data = await adminRequest("stats_sales");
        const itemsText = (Array.isArray(data.items) ? data.items : [])
          .map((item) => {
            const typeTitle = item.type === "premium" ? "Premium" : "Звёзды";
            const amountValue =
              item.type === "premium"
                ? `${formatNumber(item.amount)} мес.`
                : `${formatNumber(item.amount)} шт.`;
            return `${typeTitle}: ${amountValue} | Выручка ${formatMoney(item.revenue)} ₽ | Себестоимость ${formatMoney(item.costPrice)} ₽`;
          })
          .join("\n");
        const topText = (Array.isArray(data.topBuyers) ? data.topBuyers : [])
          .map((buyer, index) => `${index + 1}. ${buyer.username || "—"} — ${formatMoney(buyer.totalSpent)} ₽`)
          .join("\n");

        setStatsResult([
          itemsText || "Сегодня продаж пока нет.",
          "",
          `Выручка: ${formatMoney(data.totalRevenue)} ₽`,
          `Себестоимость: ${formatMoney(data.totalCostPrice)} ₽`,
          `Прибыль: ${formatMoney(data.profit)} ₽`,
          "",
          "Топ покупателей:",
          topText || "Нет данных",
        ].join("\n"));
      } catch (error) {
        setStatsResult(error.message);
      } finally {
        setButtonsState(false);
      }
    });

    usersSearchButton.addEventListener("click", async () => {
      usersQuery = usersSearchInput.value.trim();
      usersPage = 1;
      await loadUsersPage({ page: usersPage, query: usersQuery });
    });

    usersSearchInput.addEventListener("keydown", (event) => {
      if (event.key !== "Enter") {
        return;
      }
      event.preventDefault();
      usersSearchButton.click();
    });

    usersPrevButton.addEventListener("click", async () => {
      if (usersLoading || usersPage <= 1) {
        return;
      }
      await loadUsersPage({ page: usersPage - 1, query: usersQuery });
    });

    usersNextButton.addEventListener("click", async () => {
      if (usersLoading || usersPage >= usersTotalPages) {
        return;
      }
      await loadUsersPage({ page: usersPage + 1, query: usersQuery });
    });

    clearButton.addEventListener("click", async () => {
      if (!window.confirm("Очистить статистику продаж за последние 24 часа?")) {
        return;
      }
      setButtonsState(true);
      try {
        const data = await adminRequest("stats_clear");
        setStatsResult(
          data.deletedCount > 0
            ? `Готово. Удалено записей: ${formatNumber(data.deletedCount)}`
            : "За последние 24 часа продаж не было."
        );
      } catch (error) {
        setStatsResult(error.message);
      } finally {
        setButtonsState(false);
      }
    });
  };
  const renderPromosSection = async () => {
    adminSectionContent.innerHTML = `
      <div class="admin-form-grid">
        <label class="admin-form-field"><span class="admin-form-label">Код</span><input class="admin-input" id="adminPromoCodeInput" type="text" maxlength="32" placeholder="NEWYEAR2026"></label>
        <label class="admin-form-field"><span class="admin-form-label">Тип эффекта</span><select class="admin-select" id="adminPromoEffectTypeInput"><option value="discount_percent">Скидка %</option><option value="free_stars">Бесплатные звёзды</option></select></label>
        <label class="admin-form-field"><span class="admin-form-label" id="adminPromoEffectValueLabel">Скидка %</span><input class="admin-input" id="adminPromoEffectValueInput" type="number" min="1" step="1" value="10"></label>
        <label class="admin-form-field"><span class="admin-form-label">Мин. звёзд</span><input class="admin-input" id="adminPromoMinStarsInput" type="number" min="0" step="1" value="0"></label>
        <label class="admin-form-field"><span class="admin-form-label">Действует до</span><input class="admin-input" id="adminPromoExpireInput" type="date"></label>
        <label class="admin-form-field"><span class="admin-form-label">Макс. активаций</span><input class="admin-input" id="adminPromoMaxUsesInput" type="number" min="1" step="1" value="100"></label>
        <label class="admin-form-field"><span class="admin-form-label">На пользователя</span><input class="admin-input" id="adminPromoPerUserInput" type="number" min="1" step="1" value="1"></label>
        <label class="admin-form-field"><span class="admin-form-label">Условие</span><select class="admin-select" id="adminPromoConditionInput"><option value="all">Всем</option><option value="buyers">Только покупателям</option></select></label>
        <label class="admin-form-field"><span class="admin-form-label">Применяется к</span><select class="admin-select" id="adminPromoTargetInput"><option value="stars">Звёзды</option><option value="premium">Premium</option><option value="ton">TON</option><option value="all">Всё</option></select></label>
      </div>
      <div class="admin-panel-actions">
        <button class="admin-action-button" id="adminPromoCreateButton" type="button">Создать промокод</button>
        <button class="admin-action-button" id="adminPromoRefreshButton" type="button">Обновить активные</button>
      </div>
      <div class="admin-table-wrap">
        <table class="admin-table">
          <thead><tr><th>Код</th><th>Эффект</th><th>Активации</th><th>Истекает</th><th>Тип</th></tr></thead>
          <tbody id="adminPromoTableBody"><tr><td colspan="5">Загрузка...</td></tr></tbody>
        </table>
      </div>
      <div class="admin-split">
        <input class="admin-input" id="adminPromoDeleteInput" type="text" maxlength="32" placeholder="Код для удаления">
        <button class="admin-action-button" id="adminPromoDeleteButton" type="button">Удалить промокод</button>
      </div>
      <div class="admin-result-card" id="adminPromoResult"></div>
    `;

    const codeInput = document.getElementById("adminPromoCodeInput");
    const effectTypeInput = document.getElementById("adminPromoEffectTypeInput");
    const effectValueInput = document.getElementById("adminPromoEffectValueInput");
    const effectValueLabel = document.getElementById("adminPromoEffectValueLabel");
    const minStarsInput = document.getElementById("adminPromoMinStarsInput");
    const expireInput = document.getElementById("adminPromoExpireInput");
    const maxUsesInput = document.getElementById("adminPromoMaxUsesInput");
    const perUserInput = document.getElementById("adminPromoPerUserInput");
    const conditionInput = document.getElementById("adminPromoConditionInput");
    const targetInput = document.getElementById("adminPromoTargetInput");
    const createButton = document.getElementById("adminPromoCreateButton");
    const refreshButton = document.getElementById("adminPromoRefreshButton");
    const deleteInput = document.getElementById("adminPromoDeleteInput");
    const deleteButton = document.getElementById("adminPromoDeleteButton");
    const tableBody = document.getElementById("adminPromoTableBody");
    const resultNode = document.getElementById("adminPromoResult");
    if (
      !codeInput ||
      !effectTypeInput ||
      !effectValueInput ||
      !effectValueLabel ||
      !minStarsInput ||
      !expireInput ||
      !maxUsesInput ||
      !perUserInput ||
      !conditionInput ||
      !targetInput ||
      !createButton ||
      !refreshButton ||
      !deleteInput ||
      !deleteButton ||
      !tableBody ||
      !resultNode
    ) {
      return;
    }

    const updateEffectInputs = () => {
      const effectType = effectTypeInput.value;
      if (effectType === "free_stars") {
        effectValueLabel.textContent = "Бесплатные звёзды";
        effectValueInput.min = "1";
        effectValueInput.max = "";
        effectValueInput.placeholder = "Например 100";
      } else {
        effectValueLabel.textContent = "Скидка %";
        effectValueInput.min = "1";
        effectValueInput.max = "100";
        effectValueInput.placeholder = "От 1 до 100";
      }
    };

    expireInput.value = toIsoDate(new Date(Date.now() + 1000 * 60 * 60 * 24 * 30));
    updateEffectInputs();
    effectTypeInput.addEventListener("change", updateEffectInputs);

    const loadPromos = async () => {
      tableBody.innerHTML = '<tr><td colspan="5">Загрузка...</td></tr>';
      try {
        const data = await adminRequest("promo_list");
        const promos = Array.isArray(data.promos) ? data.promos : [];
        if (!promos.length) {
          tableBody.innerHTML = '<tr><td colspan="5">Активных промокодов нет</td></tr>';
          return;
        }
        tableBody.innerHTML = promos
          .map((promo) => {
            const remaining = Math.max(0, Number(promo.maxUses || 0) - Number(promo.usesCount || 0));
            const effectType = String(promo.effectType || "discount_percent");
            const effectValue = Number.parseInt(promo.effectValue, 10);
            const effectText =
              effectType === "free_stars"
                ? `Бесплатно ${Number.isFinite(effectValue) ? effectValue : 0}⭐`
                : `${Number.isFinite(effectValue) ? effectValue : Number.parseInt(promo.discount, 10) || 0}%`;
            return `<tr><td><code>${escapeHtml(promo.code || "")}</code></td><td>${escapeHtml(effectText)}</td><td>${escapeHtml(String(promo.usesCount || 0))}/${escapeHtml(String(promo.maxUses || 0))} (ост: ${escapeHtml(String(remaining))})</td><td>${escapeHtml(String(promo.expiresAt || "—"))}</td><td>${escapeHtml(String(promo.target || "stars"))}</td></tr>`;
          })
          .join("");
      } catch (error) {
        tableBody.innerHTML = `<tr><td colspan="5">${escapeHtml(error.message)}</td></tr>`;
      }
    };

    createButton.addEventListener("click", async () => {
      createButton.disabled = true;
      try {
        const payload = {
          code: codeInput.value.trim().toUpperCase(),
          effectType: effectTypeInput.value,
          effectValue: Number.parseInt(effectValueInput.value, 10),
          minStars: Number.parseInt(minStarsInput.value, 10),
          expiresAt: expireInput.value,
          maxUses: Number.parseInt(maxUsesInput.value, 10),
          maxUsesPerUser: Number.parseInt(perUserInput.value, 10),
          condition: conditionInput.value,
          target: targetInput.value,
        };
        await adminRequest("promo_create", payload);
        const maxUsesText = Number.isFinite(payload.maxUses) && payload.maxUses > 0 ? payload.maxUses : "—";
        resultNode.textContent = `Промокод ${payload.code || "—"} создан. Активации: 0/${maxUsesText}`;
        await loadPromos();
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        createButton.disabled = false;
      }
    });

    refreshButton.addEventListener("click", () => {
      loadPromos().catch(() => {});
    });

    deleteButton.addEventListener("click", async () => {
      const code = deleteInput.value.trim().toUpperCase();
      if (!code) {
        resultNode.textContent = "Введите код для удаления";
        return;
      }
      if (!window.confirm(`Удалить промокод ${code}?`)) {
        return;
      }

      deleteButton.disabled = true;
      try {
        await adminRequest("promo_delete", { code });
        resultNode.textContent = `Промокод ${code} удалён`;
        deleteInput.value = "";
        await loadPromos();
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        deleteButton.disabled = false;
      }
    });

    await loadPromos();
  };
  const renderReferralsSection = async () => {
    adminSectionContent.innerHTML = `
      <div class="admin-split">
        <input class="admin-input" id="adminRefUserIdInput" type="number" min="1" step="1" placeholder="ID пользователя">
        <button class="admin-action-button" id="adminRefShowButton" type="button">Показать рефералов</button>
      </div>
      <button class="admin-action-button" id="adminRefResetButton" type="button">Сбросить активных</button>
      <div class="admin-result-card" id="adminRefResult">Введите ID пользователя.</div>
    `;

    const userIdInput = document.getElementById("adminRefUserIdInput");
    const showButton = document.getElementById("adminRefShowButton");
    const resetButton = document.getElementById("adminRefResetButton");
    const resultNode = document.getElementById("adminRefResult");
    if (!userIdInput || !showButton || !resetButton || !resultNode) {
      return;
    }

    const getUserId = () => Number.parseInt(userIdInput.value, 10);

    showButton.addEventListener("click", async () => {
      const userId = getUserId();
      if (!Number.isFinite(userId) || userId <= 0) {
        resultNode.textContent = "Введите корректный ID пользователя";
        return;
      }

      showButton.disabled = true;
      resetButton.disabled = true;
      try {
        const data = await adminRequest("referrals_show", { userId });
        const usernames = Array.isArray(data.usernames) ? data.usernames : [];
        resultNode.textContent =
          usernames.length > 0
            ? `Активные рефералы (${usernames.length}):\n${usernames.map((name) => `@${name}`).join("\n")}`
            : `У пользователя ${userId} нет активных рефералов`;
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        showButton.disabled = false;
        resetButton.disabled = false;
      }
    });

    resetButton.addEventListener("click", async () => {
      const userId = getUserId();
      if (!Number.isFinite(userId) || userId <= 0) {
        resultNode.textContent = "Введите корректный ID пользователя";
        return;
      }
      if (!window.confirm(`Сбросить активных рефералов у пользователя ${userId}?`)) {
        return;
      }

      showButton.disabled = true;
      resetButton.disabled = true;
      try {
        const data = await adminRequest("referrals_reset", { userId });
        resultNode.textContent = `Сброшено активных рефералов: ${formatNumber(data.resetCount)}`;
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        showButton.disabled = false;
        resetButton.disabled = false;
      }
    });
  };
  const renderStarsSection = async () => {
    adminSectionContent.innerHTML = `
      <div class="admin-result-card" id="adminStarsTotalResult">Загрузка...</div>
      <div class="admin-split">
        <input class="admin-input" id="adminStarsAmountInput" type="number" min="1" step="1" placeholder="Количество звёзд">
        <button class="admin-action-button" id="adminStarsRefreshButton" type="button">Обновить</button>
      </div>
      <div class="admin-panel-actions">
        <button class="admin-action-button" id="adminStarsAddButton" type="button">Добавить</button>
        <button class="admin-action-button" id="adminStarsRemoveButton" type="button">Убрать</button>
      </div>
      <div class="admin-form-grid">
        <label class="admin-form-field"><span class="admin-form-label">50 - 75</span><input class="admin-input" id="adminRate50_75" type="number" min="0.01" step="0.001"></label>
        <label class="admin-form-field"><span class="admin-form-label">76 - 100</span><input class="admin-input" id="adminRate76_100" type="number" min="0.01" step="0.001"></label>
        <label class="admin-form-field"><span class="admin-form-label">101 - 250</span><input class="admin-input" id="adminRate101_250" type="number" min="0.01" step="0.001"></label>
        <label class="admin-form-field"><span class="admin-form-label">251+</span><input class="admin-input" id="adminRate251_plus" type="number" min="0.01" step="0.001"></label>
      </div>
      <button class="admin-action-button" id="adminSaveRatesButton" type="button">Сохранить тарифы</button>
      <div class="admin-result-card" id="adminStarsResult"></div>
    `;

    const totalNode = document.getElementById("adminStarsTotalResult");
    const amountInput = document.getElementById("adminStarsAmountInput");
    const refreshButton = document.getElementById("adminStarsRefreshButton");
    const addButton = document.getElementById("adminStarsAddButton");
    const removeButton = document.getElementById("adminStarsRemoveButton");
    const saveRatesButton = document.getElementById("adminSaveRatesButton");
    const resultNode = document.getElementById("adminStarsResult");
    const rateInputs = {
      "50_75": document.getElementById("adminRate50_75"),
      "76_100": document.getElementById("adminRate76_100"),
      "101_250": document.getElementById("adminRate101_250"),
      "251_plus": document.getElementById("adminRate251_plus"),
    };
    if (
      !totalNode ||
      !amountInput ||
      !refreshButton ||
      !addButton ||
      !removeButton ||
      !saveRatesButton ||
      !resultNode
    ) {
      return;
    }

    const setButtonsState = (disabled) => {
      refreshButton.disabled = disabled;
      addButton.disabled = disabled;
      removeButton.disabled = disabled;
      saveRatesButton.disabled = disabled;
    };

    const applyRatesToInputs = (rates) => {
      Object.entries(rateInputs).forEach(([key, input]) => {
        if (!(input instanceof HTMLInputElement)) {
          return;
        }
        const numeric = Number(rates?.[key]);
        input.value = Number.isFinite(numeric) && numeric > 0 ? String(Number(numeric.toFixed(3))) : "";
      });
    };

    const collectRatesPayload = () => {
      const payload = {};
      for (const [key, input] of Object.entries(rateInputs)) {
        const numeric = Number(String(input?.value || "").replace(",", ".").trim());
        if (!Number.isFinite(numeric) || numeric <= 0) {
          return null;
        }
        payload[key] = Number(numeric.toFixed(3));
      }
      return payload;
    };

    const loadData = async () => {
      const data = await adminRequest("stars_get");
      totalNode.textContent = `Всего продано звёзд: ${formatNumber(data.totalStars)}`;
      applyRatesToInputs(data.starRates || {});
    };

    const updateTotal = async (mode) => {
      const amount = Number.parseInt(amountInput.value, 10);
      if (!Number.isFinite(amount) || amount <= 0) {
        throw new Error("Введите корректное количество звёзд");
      }
      const data = await adminRequest("stars_update", { mode, amount });
      totalNode.textContent = `Всего продано звёзд: ${formatNumber(data.totalStars)}`;
      resultNode.textContent = mode === "add" ? `Добавлено: ${formatNumber(amount)}` : `Убрано: ${formatNumber(amount)}`;
    };

    refreshButton.addEventListener("click", async () => {
      setButtonsState(true);
      try {
        await loadData();
        resultNode.textContent = "Данные обновлены";
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        setButtonsState(false);
      }
    });

    addButton.addEventListener("click", async () => {
      setButtonsState(true);
      try {
        await updateTotal("add");
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        setButtonsState(false);
      }
    });

    removeButton.addEventListener("click", async () => {
      setButtonsState(true);
      try {
        await updateTotal("remove");
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        setButtonsState(false);
      }
    });

    saveRatesButton.addEventListener("click", async () => {
      const ratesPayload = collectRatesPayload();
      if (!ratesPayload) {
        resultNode.textContent = "Введите корректные тарифы больше нуля";
        return;
      }

      setButtonsState(true);
      try {
        const data = await requestAdminRates(ratesPayload);
        applyRatesToInputs(data.starRates || ratesPayload);
        window.starslixMiniappConfig = {
          ...(window.starslixMiniappConfig || {}),
          starRates: { ...(data.starRates || ratesPayload) },
        };
        window.dispatchEvent(new CustomEvent("starslix:pricing-updated"));
        resultNode.textContent = "Тарифы сохранены";
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        setButtonsState(false);
      }
    });

    try {
      await loadData();
      resultNode.textContent = "Данные загружены";
    } catch (error) {
      totalNode.textContent = "Не удалось загрузить данные";
      resultNode.textContent = error.message;
    }
  };
  const renderPaymentSection = async () => {
    adminSectionContent.innerHTML = `
      <div class="admin-panel-actions">
        <button class="admin-action-button" id="adminPaymentSummaryButton" type="button">Обновить баланс</button>
        <button class="admin-action-button" id="adminPaymentHistoryButton" type="button">История пополнений</button>
      </div>
      <div class="admin-result-card" id="adminPaymentSummaryResult">Загрузка...</div>
      <div class="admin-split">
        <input class="admin-input" id="adminPaymentUserIdInput" type="text" placeholder="ID получателя выплаты">
        <input class="admin-input" id="adminPaymentAmountInput" type="number" min="0.01" step="0.01" placeholder="Сумма выплаты">
      </div>
      <button class="admin-action-button" id="adminPaymentWithdrawButton" type="button">Сделать выплату</button>
      <div class="admin-table-wrap">
        <table class="admin-table">
          <thead><tr><th>Bill ID</th><th>Сумма</th><th>Описание</th><th>Дата</th><th>User ID</th></tr></thead>
          <tbody id="adminPaymentHistoryBody"><tr><td colspan="5">—</td></tr></tbody>
        </table>
      </div>
      <div class="admin-result-card" id="adminPaymentResult"></div>
    `;

    const summaryButton = document.getElementById("adminPaymentSummaryButton");
    const historyButton = document.getElementById("adminPaymentHistoryButton");
    const summaryNode = document.getElementById("adminPaymentSummaryResult");
    const userIdInput = document.getElementById("adminPaymentUserIdInput");
    const amountInput = document.getElementById("adminPaymentAmountInput");
    const withdrawButton = document.getElementById("adminPaymentWithdrawButton");
    const historyBody = document.getElementById("adminPaymentHistoryBody");
    const resultNode = document.getElementById("adminPaymentResult");
    if (
      !summaryButton ||
      !historyButton ||
      !summaryNode ||
      !userIdInput ||
      !amountInput ||
      !withdrawButton ||
      !historyBody ||
      !resultNode
    ) {
      return;
    }

    const setButtonsState = (disabled) => {
      summaryButton.disabled = disabled;
      historyButton.disabled = disabled;
      withdrawButton.disabled = disabled;
    };

    const loadSummary = async () => {
      const data = await adminRequest("payment_summary");
      summaryNode.textContent = [
        `Баланс: ${formatMoney(data.balance)} ₽`,
        `Оплаченных платежей: ${formatNumber(data.paidCount)}`,
        `Сумма оплат: ${formatMoney(data.paidAmount)} ₽`,
      ].join("\n");
    };

    const loadHistory = async () => {
      historyBody.innerHTML = '<tr><td colspan="5">Загрузка...</td></tr>';
      const data = await adminRequest("payment_history", { limit: 10 });
      const items = Array.isArray(data.items) ? data.items : [];
      if (!items.length) {
        historyBody.innerHTML = '<tr><td colspan="5">История пуста</td></tr>';
        return;
      }
      historyBody.innerHTML = items
        .map(
          (item) =>
            `<tr><td>${escapeHtml(item.billId || "—")}</td><td>${formatMoney(item.amount)} ₽</td><td>${escapeHtml(item.description || "—")}</td><td>${escapeHtml(item.date || "—")}</td><td>${escapeHtml(String(item.userId || "—"))}</td></tr>`
        )
        .join("");
    };

    summaryButton.addEventListener("click", async () => {
      setButtonsState(true);
      try {
        await loadSummary();
        resultNode.textContent = "Баланс обновлён";
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        setButtonsState(false);
      }
    });

    historyButton.addEventListener("click", async () => {
      setButtonsState(true);
      try {
        await loadHistory();
        resultNode.textContent = "История обновлена";
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        setButtonsState(false);
      }
    });

    withdrawButton.addEventListener("click", async () => {
      const userId = userIdInput.value.trim();
      const amount = Number(String(amountInput.value || "").replace(",", "."));
      if (!userId) {
        resultNode.textContent = "Введите ID получателя выплаты";
        return;
      }
      if (!Number.isFinite(amount) || amount <= 0) {
        resultNode.textContent = "Введите корректную сумму выплаты";
        return;
      }

      setButtonsState(true);
      try {
        const data = await adminRequest("payment_withdraw", { userId, amount });
        resultNode.textContent = data.success ? "Запрос на выплату отправлен" : "Не удалось выполнить выплату";
        await loadSummary();
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        setButtonsState(false);
      }
    });

    try {
      await loadSummary();
      resultNode.textContent = "Данные загружены";
    } catch (error) {
      summaryNode.textContent = "Не удалось загрузить баланс";
      resultNode.textContent = error.message;
    }
  };
  const renderBroadcastSection = async () => {
    adminSectionContent.innerHTML = `
      <label class="admin-form-field"><span class="admin-form-label">Текст рассылки</span><textarea class="admin-textarea" id="adminBroadcastTextInput" placeholder="Введите текст рассылки"></textarea></label>
      <div class="admin-upload">
        <label class="admin-upload-label" for="adminBroadcastImageInput">Выбрать фото</label>
        <input id="adminBroadcastImageInput" type="file" accept="image/*" hidden>
        <span class="admin-upload-name" id="adminBroadcastImageName">Фото не выбрано</span>
      </div>
      <label class="admin-form-field"><span class="admin-form-label">Кнопки (каждая строка: Текст - https://ссылка)</span><textarea class="admin-textarea" id="adminBroadcastButtonsInput" placeholder="Наш сайт - https://example.com"></textarea></label>
      <button class="admin-action-button" id="adminBroadcastSendButton" type="button">Отправить рассылку</button>
      <div class="admin-result-card" id="adminBroadcastResult">Можно отправить текст, фото или текст с фото.</div>
    `;

    const textInput = document.getElementById("adminBroadcastTextInput");
    const imageInput = document.getElementById("adminBroadcastImageInput");
    const imageNameNode = document.getElementById("adminBroadcastImageName");
    const buttonsInput = document.getElementById("adminBroadcastButtonsInput");
    const sendButton = document.getElementById("adminBroadcastSendButton");
    const resultNode = document.getElementById("adminBroadcastResult");
    if (!textInput || !imageInput || !imageNameNode || !buttonsInput || !sendButton || !resultNode) {
      return;
    }

    imageInput.addEventListener("change", () => {
      const file = imageInput.files?.[0];
      imageNameNode.textContent = file ? file.name : "Фото не выбрано";
    });

    sendButton.addEventListener("click", async () => {
      const text = textInput.value.trim();
      const file = imageInput.files?.[0] || null;
      if (!text && !file) {
        resultNode.textContent = "Добавьте текст и/или фото для рассылки";
        return;
      }

      let buttons = [];
      try {
        buttons = parseButtonsText(buttonsInput.value);
      } catch (error) {
        resultNode.textContent = error.message;
        return;
      }

      sendButton.disabled = true;
      try {
        let imageBase64 = "";
        let imageMime = "";
        let imageName = "";
        if (file) {
          const dataUrl = await fileToDataUrl(file);
          imageBase64 = stripDataUrlPrefix(dataUrl);
          imageMime = String(file.type || "").trim();
          imageName = String(file.name || "").trim();
        }

        const data = await adminRequest("broadcast_send", {
          text,
          buttons,
          imageBase64,
          imageMime,
          imageName,
        });

        resultNode.textContent = `Рассылка завершена\nУспешно: ${formatNumber(data.success)}\nОшибок: ${formatNumber(data.fail)}`;
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        sendButton.disabled = false;
      }
    });
  };
  const renderFindUserSection = async () => {
    adminSectionContent.innerHTML = `
      <div class="admin-split">
        <input class="admin-input" id="adminFindUserQueryInput" type="text" placeholder="Username или ID">
        <button class="admin-action-button" id="adminFindUserButton" type="button">Найти пользователя</button>
      </div>
      <div class="admin-result-card" id="adminFindUserResult">Введите username (например @example) или ID.</div>
    `;

    const queryInput = document.getElementById("adminFindUserQueryInput");
    const findButton = document.getElementById("adminFindUserButton");
    const resultNode = document.getElementById("adminFindUserResult");
    if (!queryInput || !findButton || !resultNode) {
      return;
    }

    findButton.addEventListener("click", async () => {
      const query = queryInput.value.trim();
      if (!query) {
        resultNode.textContent = "Введите username или ID";
        return;
      }

      findButton.disabled = true;
      try {
        const data = await adminRequest("user_find", { query });
        const user = data.user || {};
        const purchases = Array.isArray(data.purchases) ? data.purchases : [];
        const promos = Array.isArray(data.usedPromos) ? data.usedPromos : [];
        const payments = Array.isArray(data.payments) ? data.payments : [];

        const purchaseText = purchases.length
          ? purchases.map((item) => `${item.itemType}: ${item.amount} за ${formatMoney(item.cost)} ₽ (${item.createdAt || "—"})`).join("\n")
          : "Нет покупок";

        const paymentText = payments.length
          ? payments.map((item) => `${formatMoney(item.amount)} ₽ - ${item.status || "—"} (${item.date || "—"})`).join("\n")
          : "Нет платежей";

        resultNode.textContent = [
          `ID: ${user.id || "—"}`,
          `Username: ${user.username || "—"}`,
          `Имя: ${[user.firstName || "", user.lastName || ""].join(" ").trim() || "—"}`,
          `Реф. код: ${user.refCode || "—"}`,
          `Рефералов: ${formatNumber(user.referralsCount || 0)} (с покупкой: ${formatNumber(user.referralsWithPurchase || 0)})`,
          "",
          "Покупки:",
          purchaseText,
          "",
          `Промокоды: ${promos.length ? promos.join(", ") : "Нет"}`,
          "",
          "Платежи:",
          paymentText,
        ].join("\n");
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        findButton.disabled = false;
      }
    });
  };

  const renderAddStarsSection = async () => {
    adminSectionContent.innerHTML = `
      <div class="admin-form-grid">
        <label class="admin-form-field"><span class="admin-form-label">ID пользователя</span><input class="admin-input" id="adminGrantUserIdInput" type="number" min="1" step="1" placeholder="Например 123456789"></label>
        <label class="admin-form-field"><span class="admin-form-label">Количество звёзд</span><input class="admin-input" id="adminGrantAmountInput" type="number" min="1" step="1" placeholder="Например 100"></label>
      </div>
      <button class="admin-action-button" id="adminGrantSubmitButton" type="button">Начислить звёзды</button>
      <div class="admin-result-card" id="adminGrantResult"></div>
    `;

    const userIdInput = document.getElementById("adminGrantUserIdInput");
    const amountInput = document.getElementById("adminGrantAmountInput");
    const submitButton = document.getElementById("adminGrantSubmitButton");
    const resultNode = document.getElementById("adminGrantResult");
    if (!userIdInput || !amountInput || !submitButton || !resultNode) {
      return;
    }

    submitButton.addEventListener("click", async () => {
      const userId = Number.parseInt(userIdInput.value, 10);
      const amount = Number.parseInt(amountInput.value, 10);
      if (!Number.isFinite(userId) || userId <= 0) {
        resultNode.textContent = "Введите корректный ID пользователя";
        return;
      }
      if (!Number.isFinite(amount) || amount <= 0) {
        resultNode.textContent = "Введите корректное количество звёзд";
        return;
      }

      submitButton.disabled = true;
      try {
        const data = await adminRequest("add_stars", { userId, amount });
        resultNode.textContent = `Начислено ${formatNumber(data.amount)} звёзд пользователю ${data.userId}. Уведомление: ${data.notificationSent ? "отправлено" : "не отправлено"}.`;
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        submitButton.disabled = false;
      }
    });
  };

  const renderAdminsSection = async () => {
    adminSectionContent.innerHTML = `
      <div class="admin-split">
        <input class="admin-input" id="adminManageUserIdInput" type="number" min="1" step="1" placeholder="ID пользователя">
        <button class="admin-action-button" id="adminManageRefreshButton" type="button">Обновить список</button>
      </div>
      <div class="admin-panel-actions">
        <button class="admin-action-button" id="adminManageAddButton" type="button">Добавить админа</button>
        <button class="admin-action-button" id="adminManageRemoveButton" type="button">Убрать админа</button>
      </div>
      <div class="admin-table-wrap">
        <table class="admin-table admin-table--compact">
          <thead><tr><th>Тг ID</th><th>Тг юз</th><th>Роль</th></tr></thead>
          <tbody id="adminManageTableBody"><tr><td colspan="3">Загрузка...</td></tr></tbody>
        </table>
      </div>
      <div class="admin-result-card" id="adminManageResult">Только владелец может добавлять и удалять админов.</div>
    `;

    const userIdInput = document.getElementById("adminManageUserIdInput");
    const refreshButton = document.getElementById("adminManageRefreshButton");
    const addButton = document.getElementById("adminManageAddButton");
    const removeButton = document.getElementById("adminManageRemoveButton");
    const tableBody = document.getElementById("adminManageTableBody");
    const resultNode = document.getElementById("adminManageResult");
    if (!userIdInput || !refreshButton || !addButton || !removeButton || !tableBody || !resultNode) {
      return;
    }

    let canManage = false;

    const setButtonsState = (disabled) => {
      refreshButton.disabled = disabled;
      addButton.disabled = disabled || !canManage;
      removeButton.disabled = disabled || !canManage;
      userIdInput.disabled = disabled || !canManage;
    };

    const renderTable = (items) => {
      if (!items.length) {
        tableBody.innerHTML = '<tr><td colspan="3">Админы не найдены</td></tr>';
        return;
      }
      tableBody.innerHTML = items
        .map((item) => {
          const idText = escapeHtml(String(item?.id ?? "—"));
          const usernameRaw = String(item?.username || "").trim().replace(/^@+/, "");
          const usernameText = usernameRaw ? `@${escapeHtml(usernameRaw)}` : "—";
          const roleText = item?.isOwner ? "Владелец" : "Админ";
          return `<tr><td>${idText}</td><td>${usernameText}</td><td>${roleText}</td></tr>`;
        })
        .join("");
    };

    const loadAdmins = async () => {
      setButtonsState(true);
      tableBody.innerHTML = '<tr><td colspan="3">Загрузка...</td></tr>';
      try {
        const data = await adminRequest("admins_list");
        canManage = Boolean(data.canManage);
        renderTable(Array.isArray(data.items) ? data.items : []);
        resultNode.textContent = canManage
          ? "Вы можете добавлять и удалять админов."
          : "Только владелец может добавлять и удалять админов.";
      } catch (error) {
        tableBody.innerHTML = `<tr><td colspan="3">${escapeHtml(error.message)}</td></tr>`;
        resultNode.textContent = error.message;
      } finally {
        setButtonsState(false);
      }
    };

    addButton.addEventListener("click", async () => {
      const userId = Number.parseInt(userIdInput.value, 10);
      if (!Number.isFinite(userId) || userId <= 0) {
        resultNode.textContent = "Введите корректный ID пользователя";
        return;
      }
      setButtonsState(true);
      try {
        const data = await adminRequest("admin_add", { userId });
        renderTable(Array.isArray(data.items) ? data.items : []);
        resultNode.textContent = data.changed
          ? `Пользователь ${userId} добавлен в админы`
          : `Пользователь ${userId} уже админ`;
        userIdInput.value = "";
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        setButtonsState(false);
      }
    });

    removeButton.addEventListener("click", async () => {
      const userId = Number.parseInt(userIdInput.value, 10);
      if (!Number.isFinite(userId) || userId <= 0) {
        resultNode.textContent = "Введите корректный ID пользователя";
        return;
      }
      if (!window.confirm(`Убрать пользователя ${userId} из админов?`)) {
        return;
      }

      setButtonsState(true);
      try {
        const data = await adminRequest("admin_remove", { userId });
        renderTable(Array.isArray(data.items) ? data.items : []);
        resultNode.textContent = data.changed
          ? `Пользователь ${userId} удалён из админов`
          : "Изменений нет";
        userIdInput.value = "";
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        setButtonsState(false);
      }
    });

    refreshButton.addEventListener("click", () => {
      loadAdmins().catch(() => {});
    });

    await loadAdmins();
  };

  sectionRenderers = {
    stats: renderStatsSection,
    promos: renderPromosSection,
    referrals: renderReferralsSection,
    stars: renderStarsSection,
    payment: renderPaymentSection,
    broadcast: renderBroadcastSection,
    "find-user": renderFindUserSection,
    "add-stars": renderAddStarsSection,
    admins: renderAdminsSection,
  };

  adminHubCloseButton?.addEventListener("click", (event) => {
    event.preventDefault();
    navigateToScreen("profile");
  });

  adminSectionCloseButton?.addEventListener("click", (event) => {
    event.preventDefault();
    navigateToScreen("admin");
  });

  sectionButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const sectionKey = button.dataset.adminSection || "";
      openSection(sectionKey).catch((error) => {
        showAlert(error.message);
      });
    });
  });
})();

(() => {
  const block = (event) => {
    event.preventDefault();
  };

  document.addEventListener("copy", block);
  document.addEventListener("cut", block);
  document.addEventListener("contextmenu", block);
  document.addEventListener("dragstart", block);

  document.addEventListener("selectstart", (event) => {
    const target = event.target;
    if (target instanceof Element && target.closest("input, textarea")) {
      return;
    }

    event.preventDefault();
  });

  document.addEventListener("keydown", (event) => {
    if (!(event.ctrlKey || event.metaKey)) {
      return;
    }

    const key = event.key.toLowerCase();
    if (key === "c" || key === "x") {
      event.preventDefault();
    }
  });
})();

