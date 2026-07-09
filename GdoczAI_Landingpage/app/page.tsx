import Nav from "@/components/Nav";
import Hero from "@/components/Hero";
import TrustBar from "@/components/TrustBar";
import Definition from "@/components/Definition";
import Results from "@/components/Results";
import HowItWorks from "@/components/HowItWorks";
import Security from "@/components/Security";
import Production from "@/components/Production";
import Editions from "@/components/Editions";
import UseCases from "@/components/UseCases";
import Integrations from "@/components/Integrations";
import TwoWays from "@/components/TwoWays";
import RoiCalculator from "@/components/RoiCalculator";
import Faq from "@/components/Faq";
import CtaFinal from "@/components/CtaFinal";
import Footer from "@/components/Footer";

export const dynamic = "force-static";

export default function Home() {
  return (
    <>
      <Nav />
      <div className="fade-in-up">
        <Hero />
      </div>
      <div className="reveal">
        <TrustBar />
      </div>
      <div className="reveal">
        <Definition />
      </div>
      <div className="reveal">
        <Results />
      </div>
      <div className="reveal">
        <HowItWorks />
      </div>
      <div className="reveal">
        <Security />
      </div>
      <div className="reveal">
        <Production />
      </div>
      <div className="reveal">
        <Editions />
      </div>
      <div className="reveal">
        <UseCases />
      </div>
      <div className="reveal">
        <Integrations />
      </div>
      <div className="reveal">
        <TwoWays />
      </div>
      <div className="reveal">
        <RoiCalculator />
      </div>
      <div className="reveal">
        <Faq />
      </div>
      <div className="reveal">
        <CtaFinal />
      </div>
      <Footer />
    </>
  );
}
