#ifndef GCC_COMPAT_H
#define GCC_COMPAT_H

#include <version>
#include <ranges>
#include <vector>
#include <string>
#include <limits>
#include <type_traits>

// GCC 13 不完全支持 std::ranges::to，提供兼容实现
#if __GNUC__ < 14

namespace std {
namespace ranges {

// 前向声明
template<typename Container>
struct to_closure;

// 通用 to 实现 - 直接调用语法: std::ranges::to<Container>(range)
template<typename Container, std::ranges::range Range>
Container to(Range&& range) {
    Container result;
    if constexpr (requires { result.reserve(std::ranges::size(range)); }) {
        result.reserve(std::ranges::size(range));
    }
    for (auto&& elem : std::forward<Range>(range)) {
        if constexpr (std::is_same_v<Container, std::string>) {
            result.push_back(static_cast<char>(elem));
        } else {
            result.push_back(std::forward<decltype(elem)>(elem));
        }
    }
    return result;
}

// 特化: to<std::vector>(range) - 推导元素类型
template<template<typename...> class Container, std::ranges::range Range>
auto to(Range&& range) -> Container<std::ranges::range_value_t<Range>> {
    Container<std::ranges::range_value_t<Range>> result;
    if constexpr (requires { result.reserve(std::ranges::size(range)); }) {
        result.reserve(std::ranges::size(range));
    }
    for (auto&& elem : std::forward<Range>(range)) {
        result.push_back(std::forward<decltype(elem)>(elem));
    }
    return result;
}

// 管道适配器 - 具体类型
template<typename Container>
struct to_closure {
    template<std::ranges::range Range>
    friend Container operator|(Range&& range, to_closure) {
        return to<Container>(std::forward<Range>(range));
    }
};

// 管道适配器 - 模板类型（用于 to<std::vector>()）
template<template<typename...> class Container>
struct to_template_closure {
    template<std::ranges::range Range>
    friend auto operator|(Range&& range, to_template_closure) {
        return to<Container>(std::forward<Range>(range));
    }
};

// 管道语法: std::ranges::to<std::string>()
template<typename Container>
to_closure<Container> to() {
    return {};
}

// 管道语法: std::ranges::to<std::vector>()
template<template<typename...> class Container>
to_template_closure<Container> to() {
    return {};
}

} // namespace ranges
} // namespace std

#endif // __GNUC__ < 14

#endif // GCC_COMPAT_H
