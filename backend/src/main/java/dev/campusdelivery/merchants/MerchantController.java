package dev.campusdelivery.merchants;

import dev.campusdelivery.config.AppProperties;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class MerchantController {

    private final MerchantRepository merchants;
    private final AppProperties props;

    public MerchantController(MerchantRepository merchants, AppProperties props) {
        this.merchants = merchants;
        this.props = props;
    }

    @GetMapping("/merchants")
    public List<MerchantView> list() {
        return merchants.findAll();
    }

    /** Map center + tunables the web clients need (staleness threshold, offer TTL). */
    @GetMapping("/config")
    public Map<String, Object> config() {
        return Map.of(
                "campus", Map.of(
                        "name", props.campus().name(),
                        "lat", props.campus().lat(),
                        "lng", props.campus().lng()),
                "staleAfterSeconds", props.location().staleAfter().toSeconds(),
                "assignmentRadiusMeters", props.assignment().radiusMeters(),
                "offerTtlSeconds", props.assignment().offerTtl().toSeconds(),
                "stuckAfterSeconds", props.dispatch().stuckAfter().toSeconds());
    }
}
