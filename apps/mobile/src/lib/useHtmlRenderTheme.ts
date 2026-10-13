import { useMemo } from "react";
import { Platform } from "react-native";
import { mobileHtmlRenderTheme } from "./htmlRenderTheme";
import { useAppearancePreferences } from "../features/settings/appearance/AppearancePreferencesProvider";

export function useHtmlRenderTheme() {
  const { themeId, themeAppearance, themeVariables, systemColorsActive } =
    useAppearancePreferences();
  return useMemo(
    () =>
      mobileHtmlRenderTheme({
        themeId,
        appearance: themeAppearance,
        variables: themeVariables,
        systemColors: systemColorsActive,
        platform: Platform.OS,
      }),
    [themeId, themeAppearance, themeVariables, systemColorsActive],
  );
}
