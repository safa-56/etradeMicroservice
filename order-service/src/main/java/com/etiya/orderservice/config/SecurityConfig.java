package com.etiya.orderservice.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.web.SecurityFilterChain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

@Configuration
public class SecurityConfig {

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        return http
                .csrf(AbstractHttpConfigurer::disable)
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(authorize -> authorize
                        .requestMatchers("/actuator/health", "/actuator/info").permitAll()
                        .requestMatchers(HttpMethod.GET, "/api/orders/**").hasRole("order_read")
                        .requestMatchers(HttpMethod.POST, "/api/orders/**").hasRole("order_write")
                        .requestMatchers(HttpMethod.PUT, "/api/orders/**").hasRole("order_write")
                        .requestMatchers(HttpMethod.DELETE, "/api/orders/**").hasRole("order_write")
                        .anyRequest().authenticated())
                .oauth2ResourceServer(oauth2 -> oauth2
                        .jwt(jwt -> jwt.jwtAuthenticationConverter(jwtAuthenticationConverter())))
                .build();
    }

    @Bean
    public JwtAuthenticationConverter jwtAuthenticationConverter() {
        JwtAuthenticationConverter converter = new JwtAuthenticationConverter();
        converter.setJwtGrantedAuthoritiesConverter(this::extractAuthorities);
        return converter;
    }

    private Collection<GrantedAuthority> extractAuthorities(Jwt jwt) {
        Collection<GrantedAuthority> authorities = new ArrayList<>();
        addRoles(authorities, realmRoles(jwt));
        resourceAccess(jwt).values().forEach(resource -> addRoles(authorities, roles(resource)));
        return authorities;
    }

    private void addRoles(Collection<GrantedAuthority> authorities, Collection<String> roles) {
        roles.stream()
                .map(role -> role.startsWith("ROLE_") ? role : "ROLE_" + role)
                .map(SimpleGrantedAuthority::new)
                .forEach(authorities::add);
    }

    private Collection<String> realmRoles(Jwt jwt) {
        Object realmAccess = jwt.getClaims().get("realm_access");
        if (realmAccess instanceof Map<?, ?> realmAccessMap) {
            return roles(realmAccessMap);
        }
        return Collections.emptyList();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Map<String, Object>> resourceAccess(Jwt jwt) {
        Object resourceAccess = jwt.getClaims().get("resource_access");
        if (resourceAccess instanceof Map<?, ?> resourceAccessMap) {
            return (Map<String, Map<String, Object>>) resourceAccessMap;
        }
        return Collections.emptyMap();
    }

    private Collection<String> roles(Map<?, ?> accessMap) {
        Object roles = accessMap.get("roles");
        if (roles instanceof Collection<?> roleCollection) {
            return roleCollection.stream()
                    .filter(String.class::isInstance)
                    .map(String.class::cast)
                    .toList();
        }
        return Collections.emptyList();
    }
}
