import { Composition } from "remotion";
import { RindexerPromo } from "./RindexerPromo";

export const RemotionVideo = () => {
  return (
    <Composition
      id="RindexerPromo"
      component={RindexerPromo}
      durationInFrames={450}
      fps={30}
      width={1280}
      height={720}
    />
  );
};
