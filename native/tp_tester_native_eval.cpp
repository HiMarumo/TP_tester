#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

namespace {

constexpr double kVelocityHeadingFallbackThresholdMps = 3.6 / 3.6;
constexpr double kApproximateRequiredStrictRatio = 0.7;
constexpr double kApproximateToleranceScale = 1.5;
constexpr double kEpsilon = 1e-9;
constexpr int kEllipseSteps = 10;

constexpr int kClassificationPedestrian = 1;
constexpr int kClassificationCyclist = 2;
constexpr int kClassificationMotorcyclist = 3;
constexpr int kClassificationCar = 4;
constexpr int kClassificationTruck = 5;
constexpr int kClassificationBus = 6;

enum class ClassGroup {
    Vehicle,
    Bike,
    Pedestrian,
};

enum class HorizonKind {
    HalfTimeRelaxed = 0,
    TimeRelaxed = 1,
};

enum class ThresholdKind {
    Approximate = 0,
    Strict = 1,
};

struct TolPair {
    double longitudinal;
    double lateral;
};

struct Point2 {
    double x;
    double y;
};

struct Step {
    double t;
    double x;
    double y;
    double yaw;
    double speed_hint;
};

struct Segment {
    double s0;
    double s1;
    double x0;
    double y0;
    double x1;
    double y1;
    double ux;
    double uy;
};

struct Curve {
    std::vector<Step> points;
    std::vector<double> s;
    std::vector<Segment> segments;
    std::vector<double> seg_s1;
};

struct Aabb {
    double min_x;
    double min_y;
    double max_x;
    double max_y;
};

struct ProfileKindData {
    std::vector<std::vector<Point2>> polys;
    std::vector<Aabb> aabbs;
    bool has_path_aabb = false;
    Aabb path_aabb{};
};

struct CollisionProfile {
    ProfileKindData hard;
    ProfileKindData soft;
};

double normalize_angle(double angle) {
    const double two_pi = 2.0 * M_PI;
    double out = std::fmod(angle + M_PI, two_pi);
    if (out < 0.0) {
        out += two_pi;
    }
    return out - M_PI;
}

ClassGroup class_group_for_classification(int classification) {
    switch (classification) {
    case kClassificationCar:
    case kClassificationTruck:
    case kClassificationBus:
        return ClassGroup::Vehicle;
    case kClassificationCyclist:
    case kClassificationMotorcyclist:
        return ClassGroup::Bike;
    case kClassificationPedestrian:
        return ClassGroup::Pedestrian;
    default:
        return ClassGroup::Vehicle;
    }
}

TolPair tolerance_start(ClassGroup group) {
    switch (group) {
    case ClassGroup::Vehicle:
        return {1.0, 0.5};
    case ClassGroup::Bike:
        return {0.5, 0.35};
    case ClassGroup::Pedestrian:
        return {0.5, 0.3};
    }
    return {1.0, 0.5};
}

TolPair tolerance_end(ClassGroup group) {
    switch (group) {
    case ClassGroup::Vehicle:
        return {7.5, 2.0};
    case ClassGroup::Bike:
        return {4.0, 1.5};
    case ClassGroup::Pedestrian:
        return {0.8, 0.5};
    }
    return {7.5, 2.0};
}

TolPair threshold_tolerances(HorizonKind horizon, double step_t, double horizon_max_t, int classification) {
    (void) horizon;
    const ClassGroup group = class_group_for_classification(classification);
    const TolPair start = tolerance_start(group);
    const TolPair end = tolerance_end(group);
    const double denom = std::max(1e-6, horizon_max_t);
    const double ratio = std::min(1.0, std::max(0.0, step_t / denom));
    return {
        std::max(1e-6, start.longitudinal + (end.longitudinal - start.longitudinal) * ratio),
        std::max(1e-6, start.lateral + (end.lateral - start.lateral) * ratio),
    };
}

std::vector<Step> build_steps(
    const double* t_arr,
    const double* x_arr,
    const double* y_arr,
    const double* yaw_arr,
    const double* speed_arr,
    int count) {
    std::vector<Step> out;
    if (!t_arr || !x_arr || !y_arr || !yaw_arr || count <= 0) {
        return out;
    }
    out.reserve(static_cast<std::size_t>(count));
    for (int i = 0; i < count; ++i) {
        out.push_back({
            t_arr[i],
            x_arr[i],
            y_arr[i],
            yaw_arr[i],
            speed_arr ? speed_arr[i] : 0.0,
        });
    }
    return out;
}

Curve build_curve(const std::vector<Step>& points) {
    Curve curve;
    curve.points = points;
    if (points.empty()) {
        return curve;
    }
    curve.s.assign(points.size(), 0.0);
    double accum = 0.0;
    for (std::size_t i = 1; i < points.size(); ++i) {
        const double dx = points[i].x - points[i - 1].x;
        const double dy = points[i].y - points[i - 1].y;
        const double seg_len = std::hypot(dx, dy);
        accum += seg_len;
        curve.s[i] = accum;
        if (seg_len > kEpsilon) {
            Segment seg;
            seg.s0 = curve.s[i - 1];
            seg.s1 = curve.s[i];
            seg.x0 = points[i - 1].x;
            seg.y0 = points[i - 1].y;
            seg.x1 = points[i].x;
            seg.y1 = points[i].y;
            seg.ux = dx / seg_len;
            seg.uy = dy / seg_len;
            curve.segments.push_back(seg);
            curve.seg_s1.push_back(seg.s1);
        }
    }
    return curve;
}

bool calc_pair_velocity(const std::vector<Step>& pts, int i0, int i1, double& dx, double& dy, double& speed) {
    const int n = static_cast<int>(pts.size());
    if (!(0 <= i0 && i0 < n && 0 <= i1 && i1 < n) || i0 == i1) {
        return false;
    }
    const double dt = pts[i1].t - pts[i0].t;
    if (dt <= 1e-6) {
        return false;
    }
    dx = pts[i1].x - pts[i0].x;
    dy = pts[i1].y - pts[i0].y;
    speed = std::hypot(dx, dy) / dt;
    return true;
}

double observed_heading_from_velocity(const std::vector<Step>& obs_pts, int idx) {
    const int n = static_cast<int>(obs_pts.size());
    const double yaw = obs_pts[static_cast<std::size_t>(idx)].yaw;
    if (n <= 1) {
        return yaw;
    }
    std::vector<std::tuple<double, double, double>> candidates;
    double dx = 0.0;
    double dy = 0.0;
    double speed = 0.0;
    if (0 < idx && idx < (n - 1) && calc_pair_velocity(obs_pts, idx - 1, idx + 1, dx, dy, speed)) {
        candidates.emplace_back(dx, dy, speed);
    }
    if (calc_pair_velocity(obs_pts, idx, idx + 1, dx, dy, speed)) {
        candidates.emplace_back(dx, dy, speed);
    }
    if (calc_pair_velocity(obs_pts, idx - 1, idx, dx, dy, speed)) {
        candidates.emplace_back(dx, dy, speed);
    }
    if (candidates.empty()) {
        return yaw;
    }
    dx = std::get<0>(candidates[0]);
    dy = std::get<1>(candidates[0]);
    speed = std::get<2>(candidates[0]);
    if (speed <= kVelocityHeadingFallbackThresholdMps || std::hypot(dx, dy) <= kEpsilon) {
        return yaw;
    }
    return std::atan2(dy, dx);
}

std::pair<double, double> project_xy_to_observed_sd(
    const Curve& curve,
    double x,
    double y,
    int anchor_idx,
    double fallback_heading) {
    if (curve.points.empty()) {
        return {0.0, 0.0};
    }
    if (curve.segments.empty()) {
        const int use_idx = std::min(std::max(0, anchor_idx), static_cast<int>(curve.points.size()) - 1);
        const Step& anchor = curve.points[static_cast<std::size_t>(use_idx)];
        const double heading = std::isfinite(fallback_heading) ? fallback_heading : anchor.yaw;
        const double tx = std::cos(heading);
        const double ty = std::sin(heading);
        const double vx = x - anchor.x;
        const double vy = y - anchor.y;
        const double s_proj = (use_idx < static_cast<int>(curve.s.size()) ? curve.s[static_cast<std::size_t>(use_idx)] : 0.0) + (tx * vx + ty * vy);
        const double d_proj = -ty * vx + tx * vy;
        return {s_proj, d_proj};
    }

    double best_dist2 = std::numeric_limits<double>::infinity();
    double best_s = 0.0;
    double best_d = 0.0;

    auto consider_candidate = [&](double base_x, double base_y, double tx, double ty, double along, double s_origin) {
        const double qx = base_x + tx * along;
        const double qy = base_y + ty * along;
        const double dx = x - qx;
        const double dy = y - qy;
        const double dist2 = dx * dx + dy * dy;
        if (!(dist2 < best_dist2)) {
            return;
        }
        best_dist2 = dist2;
        best_s = s_origin + along;
        best_d = tx * dy - ty * dx;
    };

    for (const Segment& seg : curve.segments) {
        const double seg_len = std::max(0.0, seg.s1 - seg.s0);
        const double vx = x - seg.x0;
        const double vy = y - seg.y0;
        double along = vx * seg.ux + vy * seg.uy;
        along = std::min(seg_len, std::max(0.0, along));
        consider_candidate(seg.x0, seg.y0, seg.ux, seg.uy, along, seg.s0);
    }

    const Segment& first = curve.segments.front();
    const double vx0 = x - first.x0;
    const double vy0 = y - first.y0;
    const double along0 = vx0 * first.ux + vy0 * first.uy;
    if (along0 < 0.0) {
        consider_candidate(first.x0, first.y0, first.ux, first.uy, along0, first.s0);
    }

    const Segment& last = curve.segments.back();
    const double vx1 = x - last.x1;
    const double vy1 = y - last.y1;
    const double along1 = vx1 * last.ux + vy1 * last.uy;
    if (along1 > 0.0) {
        consider_candidate(last.x1, last.y1, last.ux, last.uy, along1, last.s1);
    }
    return {best_s, best_d};
}

int nearest_time_index(
    const std::vector<Step>& points,
    double t,
    double max_dt) {
    if (points.empty()) {
        return -1;
    }
    auto it = std::lower_bound(
        points.begin(),
        points.end(),
        t,
        [](const Step& step, double value) { return step.t < value; }
    );
    int best_idx = -1;
    double best_dt = std::numeric_limits<double>::infinity();
    if (it != points.end()) {
        best_idx = static_cast<int>(std::distance(points.begin(), it));
        best_dt = std::abs(it->t - t);
    }
    if (it != points.begin()) {
        const int prev_idx = static_cast<int>(std::distance(points.begin(), it - 1));
        const double prev_dt = std::abs((it - 1)->t - t);
        if (best_idx < 0 || prev_dt < best_dt) {
            best_idx = prev_idx;
            best_dt = prev_dt;
        }
    }
    if (best_idx < 0 || best_dt > max_dt) {
        return -1;
    }
    return best_idx;
}

std::pair<bool, double> evaluate_validation_path(
    const std::vector<Step>& obs_pts,
    const std::vector<Step>& pred_pts,
    HorizonKind horizon,
    ThresholdKind threshold,
    double horizon_max_t,
    int classification,
    double max_dt) {
    if (obs_pts.empty() || pred_pts.empty()) {
        return {false, std::numeric_limits<double>::infinity()};
    }
    const Curve obs_curve = build_curve(obs_pts);
    if (obs_curve.points.empty()) {
        return {false, std::numeric_limits<double>::infinity()};
    }

    double cost = 0.0;
    int strict_ok_steps = 0;
    for (std::size_t i = 0; i < obs_pts.size(); ++i) {
        const Step& obs = obs_pts[i];
        const int pred_idx = nearest_time_index(pred_pts, obs.t, max_dt);
        if (pred_idx < 0) {
            return {false, std::numeric_limits<double>::infinity()};
        }
        const Step& pred = pred_pts[static_cast<std::size_t>(pred_idx)];
        const double fallback_heading = observed_heading_from_velocity(obs_pts, static_cast<int>(i));
        const auto sd = project_xy_to_observed_sd(
            obs_curve,
            pred.x,
            pred.y,
            static_cast<int>(i),
            fallback_heading);
        const double s_obs = i < obs_curve.s.size() ? obs_curve.s[i] : 0.0;
        const double longitudinal = sd.first - s_obs;
        const double lateral = sd.second;
        const double yaw_diff = normalize_angle(pred.yaw - obs.yaw);
        const TolPair tol = threshold_tolerances(horizon, obs.t, horizon_max_t, classification);
        const bool strict_ok = std::abs(longitudinal) <= tol.longitudinal && std::abs(lateral) <= tol.lateral;

        if (threshold == ThresholdKind::Strict) {
            if (!strict_ok) {
                return {false, std::numeric_limits<double>::infinity()};
            }
        } else {
            const double expanded_longitudinal = std::max(1e-6, tol.longitudinal * kApproximateToleranceScale);
            const double expanded_lateral = std::max(1e-6, tol.lateral * kApproximateToleranceScale);
            if (std::abs(longitudinal) > expanded_longitudinal || std::abs(lateral) > expanded_lateral) {
                return {false, std::numeric_limits<double>::infinity()};
            }
            if (strict_ok) {
                ++strict_ok_steps;
            }
        }

        cost +=
            std::pow(longitudinal / tol.longitudinal, 2.0) +
            std::pow(lateral / tol.lateral, 2.0) +
            0.05 * (yaw_diff * yaw_diff);
    }

    if (threshold == ThresholdKind::Approximate) {
        const double strict_ratio = static_cast<double>(strict_ok_steps) / static_cast<double>(obs_pts.size());
        if (strict_ratio + 1e-9 < kApproximateRequiredStrictRatio) {
            return {false, std::numeric_limits<double>::infinity()};
        }
    }
    return {true, cost};
}

std::pair<double, double> path_heading_speed(const std::vector<Step>& points, int idx) {
    const int n = static_cast<int>(points.size());
    const Step& p = points[static_cast<std::size_t>(idx)];
    const double yaw = p.yaw;
    const double speed_hint = std::abs(p.speed_hint);
    if (n <= 1) {
        return {yaw, std::max(0.0, speed_hint)};
    }

    double derived_speed = 0.0;
    bool has_heading = false;
    double derived_heading = yaw;
    const int pairs[3][2] = {{idx - 1, idx + 1}, {idx, idx + 1}, {idx - 1, idx}};
    for (const auto& pair : pairs) {
        double dx = 0.0;
        double dy = 0.0;
        double speed = 0.0;
        if (!calc_pair_velocity(points, pair[0], pair[1], dx, dy, speed)) {
            continue;
        }
        if (speed > derived_speed) {
            derived_speed = speed;
        }
        if (!has_heading && std::hypot(dx, dy) > kEpsilon && speed > 1e-6) {
            derived_heading = std::atan2(dy, dx);
            has_heading = true;
        }
    }

    const double step_speed = speed_hint > 1e-6 ? speed_hint : derived_speed;
    if (step_speed <= kVelocityHeadingFallbackThresholdMps) {
        return {yaw, std::max(0.0, step_speed)};
    }
    if (has_heading) {
        return {derived_heading, std::max(0.0, step_speed)};
    }
    return {yaw, std::max(0.0, step_speed)};
}

std::tuple<double, double, double, double> sample_collision_curve(
    const Curve& curve,
    double target_s,
    int anchor_idx,
    double fallback_heading) {
    if (curve.points.empty()) {
        const double tx = std::cos(fallback_heading);
        const double ty = std::sin(fallback_heading);
        return std::make_tuple(0.0, 0.0, tx, ty);
    }
    if (curve.segments.empty()) {
        const int use_idx = std::min(std::max(0, anchor_idx), static_cast<int>(curve.points.size()) - 1);
        const Step& anchor = curve.points[static_cast<std::size_t>(use_idx)];
        const double anchor_s = use_idx < static_cast<int>(curve.s.size()) ? curve.s[static_cast<std::size_t>(use_idx)] : 0.0;
        const double tx = std::cos(fallback_heading);
        const double ty = std::sin(fallback_heading);
        const double delta = target_s - anchor_s;
        return std::make_tuple(anchor.x + tx * delta, anchor.y + ty * delta, tx, ty);
    }
    if (target_s <= curve.segments.front().s0) {
        const Segment& seg = curve.segments.front();
        const double delta = target_s - seg.s0;
        return std::make_tuple(seg.x0 + seg.ux * delta, seg.y0 + seg.uy * delta, seg.ux, seg.uy);
    }
    if (target_s >= curve.segments.back().s1) {
        const Segment& seg = curve.segments.back();
        const double delta = target_s - seg.s1;
        return std::make_tuple(seg.x1 + seg.ux * delta, seg.y1 + seg.uy * delta, seg.ux, seg.uy);
    }
    const auto it = std::lower_bound(curve.seg_s1.begin(), curve.seg_s1.end(), target_s);
    std::size_t seg_idx = static_cast<std::size_t>(std::distance(curve.seg_s1.begin(), it));
    if (seg_idx >= curve.segments.size()) {
        seg_idx = curve.segments.size() - 1;
    }
    const Segment& seg = curve.segments[seg_idx];
    const double delta = target_s - seg.s0;
    return std::make_tuple(seg.x0 + seg.ux * delta, seg.y0 + seg.uy * delta, seg.ux, seg.uy);
}

std::vector<Point2> collider_local_points(double speed, double length, double width, bool soft) {
    const double scale = soft ? 1.1 : 1.0;
    const double major_scale = soft ? 2.0 : 1.0;
    const double rect_l = std::max(0.1, length * scale);
    const double rect_w = std::max(0.1, width * scale);
    const double half_l = 0.5 * rect_l;
    const double half_w = 0.5 * rect_w;
    const double speed_step = std::max(0.0, speed);
    double major = 0.0;
    if (speed_step > kVelocityHeadingFallbackThresholdMps) {
        major = std::max(0.0, speed_step * major_scale);
    }
    const double half_major = 0.5 * major;

    std::vector<Point2> local_pts;
    local_pts.reserve(static_cast<std::size_t>(kEllipseSteps) + 5);
    local_pts.push_back({-half_l, -half_w});
    local_pts.push_back({-half_l, +half_w});
    local_pts.push_back({+half_l, +half_w});
    if (half_major > 1e-6) {
        for (int i = 0; i <= kEllipseSteps; ++i) {
            const double ratio = static_cast<double>(i) / static_cast<double>(kEllipseSteps);
            const double theta = (M_PI * 0.5) - ratio * M_PI;
            local_pts.push_back({
                +half_l + half_major * std::cos(theta),
                half_w * std::sin(theta),
            });
        }
    }
    local_pts.push_back({+half_l, -half_w});
    return local_pts;
}

std::vector<Point2> transform_collider_local_points(
    const Curve& curve,
    int base_idx,
    double base_s,
    double fallback_heading,
    const std::vector<Point2>& local_pts) {
    std::vector<Point2> out;
    out.reserve(local_pts.size());
    for (const Point2& local_pt : local_pts) {
        double cx = 0.0;
        double cy = 0.0;
        double tx = 0.0;
        double ty = 0.0;
        std::tie(cx, cy, tx, ty) = sample_collision_curve(curve, base_s + local_pt.x, base_idx, fallback_heading);
        const double nx = -ty;
        const double ny = tx;
        out.push_back({
            cx + nx * local_pt.y,
            cy + ny * local_pt.y,
        });
    }
    return out;
}

Aabb polygon_aabb(const std::vector<Point2>& poly) {
    Aabb out{};
    if (poly.empty()) {
        return out;
    }
    out.min_x = out.max_x = poly[0].x;
    out.min_y = out.max_y = poly[0].y;
    for (const Point2& p : poly) {
        out.min_x = std::min(out.min_x, p.x);
        out.min_y = std::min(out.min_y, p.y);
        out.max_x = std::max(out.max_x, p.x);
        out.max_y = std::max(out.max_y, p.y);
    }
    return out;
}

bool aabb_overlaps(const Aabb& a, const Aabb& b) {
    return !(a.max_x < b.min_x - 1e-9 || b.max_x < a.min_x - 1e-9 || a.max_y < b.min_y - 1e-9 || b.max_y < a.min_y - 1e-9);
}

void merge_aabb(ProfileKindData& data, const Aabb& add) {
    if (!data.has_path_aabb) {
        data.path_aabb = add;
        data.has_path_aabb = true;
        return;
    }
    data.path_aabb.min_x = std::min(data.path_aabb.min_x, add.min_x);
    data.path_aabb.min_y = std::min(data.path_aabb.min_y, add.min_y);
    data.path_aabb.max_x = std::max(data.path_aabb.max_x, add.max_x);
    data.path_aabb.max_y = std::max(data.path_aabb.max_y, add.max_y);
}

double dot_project(const Point2& p, double axis_x, double axis_y) {
    return p.x * axis_x + p.y * axis_y;
}

bool polygons_intersect(const std::vector<Point2>& poly_a, const std::vector<Point2>& poly_b) {
    if (poly_a.size() < 3 || poly_b.size() < 3) {
        return false;
    }
    const std::vector<Point2>* polys[2] = {&poly_a, &poly_b};
    for (const auto* poly : polys) {
        const std::size_t n = poly->size();
        for (std::size_t i = 0; i < n; ++i) {
            const Point2& p1 = (*poly)[i];
            const Point2& p2 = (*poly)[(i + 1) % n];
            const double ex = p2.x - p1.x;
            const double ey = p2.y - p1.y;
            double nx = -ey;
            double ny = ex;
            const double norm = std::hypot(nx, ny);
            if (norm <= kEpsilon) {
                continue;
            }
            nx /= norm;
            ny /= norm;

            double a_min = dot_project(poly_a[0], nx, ny);
            double a_max = a_min;
            for (const Point2& p : poly_a) {
                const double dot = dot_project(p, nx, ny);
                a_min = std::min(a_min, dot);
                a_max = std::max(a_max, dot);
            }
            double b_min = dot_project(poly_b[0], nx, ny);
            double b_max = b_min;
            for (const Point2& p : poly_b) {
                const double dot = dot_project(p, nx, ny);
                b_min = std::min(b_min, dot);
                b_max = std::max(b_max, dot);
            }
            if (a_max < b_min - 1e-9 || b_max < a_min - 1e-9) {
                return false;
            }
        }
    }
    return true;
}

CollisionProfile build_collision_profile(const std::vector<Step>& points, double length, double width) {
    CollisionProfile profile;
    const Curve curve = build_curve(points);
    const std::size_t n = points.size();
    profile.hard.polys.reserve(n);
    profile.hard.aabbs.reserve(n);
    profile.soft.polys.reserve(n);
    profile.soft.aabbs.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        const auto heading_speed_pair = path_heading_speed(points, static_cast<int>(i));
        const double heading = heading_speed_pair.first;
        const double speed = heading_speed_pair.second;
        const double base_s = i < curve.s.size() ? curve.s[i] : 0.0;
        const auto hard_local = collider_local_points(speed, length, width, false);
        auto hard_poly = transform_collider_local_points(curve, static_cast<int>(i), base_s, heading, hard_local);
        const Aabb hard_aabb = polygon_aabb(hard_poly);
        profile.hard.polys.push_back(std::move(hard_poly));
        profile.hard.aabbs.push_back(hard_aabb);
        merge_aabb(profile.hard, hard_aabb);

        const auto soft_local = collider_local_points(speed, length, width, true);
        auto soft_poly = transform_collider_local_points(curve, static_cast<int>(i), base_s, heading, soft_local);
        const Aabb soft_aabb = polygon_aabb(soft_poly);
        profile.soft.polys.push_back(std::move(soft_poly));
        profile.soft.aabbs.push_back(soft_aabb);
        merge_aabb(profile.soft, soft_aabb);
    }
    return profile;
}

std::pair<bool, bool> evaluate_collision_pair(
    const std::vector<Step>& pred_pts,
    double pred_length,
    double pred_width,
    const std::vector<Step>& ego_pts,
    double ego_length,
    double ego_width,
    double max_dt) {
    if (pred_pts.empty() || ego_pts.empty()) {
        return {false, false};
    }
    const CollisionProfile pred_profile = build_collision_profile(pred_pts, pred_length, pred_width);
    const CollisionProfile ego_profile = build_collision_profile(ego_pts, ego_length, ego_width);
    const bool need_hard = pred_profile.hard.has_path_aabb && ego_profile.soft.has_path_aabb && aabb_overlaps(pred_profile.hard.path_aabb, ego_profile.soft.path_aabb);
    const bool need_soft = pred_profile.soft.has_path_aabb && ego_profile.soft.has_path_aabb && aabb_overlaps(pred_profile.soft.path_aabb, ego_profile.soft.path_aabb);
    if (!need_hard && !need_soft) {
        return {false, false};
    }

    bool hard = false;
    bool soft = false;
    for (std::size_t i = 0; i < pred_pts.size(); ++i) {
        const int ego_idx = nearest_time_index(ego_pts, pred_pts[i].t, max_dt);
        if (ego_idx < 0) {
            continue;
        }
        const auto& ego_soft_poly = ego_profile.soft.polys[static_cast<std::size_t>(ego_idx)];
        const auto& ego_soft_aabb = ego_profile.soft.aabbs[static_cast<std::size_t>(ego_idx)];
        if (need_hard && !hard) {
            const auto& pred_poly = pred_profile.hard.polys[i];
            const auto& pred_aabb = pred_profile.hard.aabbs[i];
            if (aabb_overlaps(pred_aabb, ego_soft_aabb) && polygons_intersect(pred_poly, ego_soft_poly)) {
                hard = true;
            }
        }
        if (need_soft && !soft) {
            const auto& pred_poly = pred_profile.soft.polys[i];
            const auto& pred_aabb = pred_profile.soft.aabbs[i];
            if (aabb_overlaps(pred_aabb, ego_soft_aabb) && polygons_intersect(pred_poly, ego_soft_poly)) {
                soft = true;
            }
        }
        if ((!need_hard || hard) && (!need_soft || soft)) {
            break;
        }
    }
    return {hard, soft};
}

}  // namespace

extern "C" {

int tp_eval_native_version() {
    return 1;
}

int tp_eval_validation_path(
    const double* obs_t,
    const double* obs_x,
    const double* obs_y,
    const double* obs_yaw,
    int obs_count,
    const double* pred_t,
    const double* pred_x,
    const double* pred_y,
    const double* pred_yaw,
    int pred_count,
    int classification,
    int horizon_kind,
    int threshold_kind,
    double horizon_max_t,
    double max_dt,
    double* out_cost) {
    if (out_cost) {
        *out_cost = std::numeric_limits<double>::infinity();
    }
    const auto obs_pts = build_steps(obs_t, obs_x, obs_y, obs_yaw, nullptr, obs_count);
    const auto pred_pts = build_steps(pred_t, pred_x, pred_y, pred_yaw, nullptr, pred_count);
    const auto result = evaluate_validation_path(
        obs_pts,
        pred_pts,
        static_cast<HorizonKind>(horizon_kind),
        static_cast<ThresholdKind>(threshold_kind),
        horizon_max_t,
        classification,
        max_dt);
    if (out_cost) {
        *out_cost = result.second;
    }
    return result.first ? 1 : 0;
}

void tp_eval_collision_results(
    const double* pred_t,
    const double* pred_x,
    const double* pred_y,
    const double* pred_yaw,
    const double* pred_speed,
    int pred_count,
    double pred_length,
    double pred_width,
    const double* ego_t,
    const double* ego_x,
    const double* ego_y,
    const double* ego_yaw,
    const double* ego_speed,
    int ego_count,
    double ego_length,
    double ego_width,
    double max_dt,
    int* out_hard,
    int* out_soft) {
    if (out_hard) {
        *out_hard = 0;
    }
    if (out_soft) {
        *out_soft = 0;
    }
    const auto pred_pts = build_steps(pred_t, pred_x, pred_y, pred_yaw, pred_speed, pred_count);
    const auto ego_pts = build_steps(ego_t, ego_x, ego_y, ego_yaw, ego_speed, ego_count);
    const auto result = evaluate_collision_pair(
        pred_pts,
        pred_length,
        pred_width,
        ego_pts,
        ego_length,
        ego_width,
        max_dt);
    if (out_hard) {
        *out_hard = result.first ? 1 : 0;
    }
    if (out_soft) {
        *out_soft = result.second ? 1 : 0;
    }
}

}
